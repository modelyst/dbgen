#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Objects related to the running of Models and ETLSteps."""
import asyncio
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import set_start_method
from os import cpu_count
from time import time
from traceback import format_exc
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

import psutil
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic.fields import PrivateAttr
from pydasher import hasher
from rich import print
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from sqlalchemy.future import Engine
from sqlmodel import Session, select

import dbgen.exceptions as exceptions
from dbgen.core.base import Base, encoders
from dbgen.core.etl_step import ETLStep
from dbgen.core.metadata import (
    ETLStepEntity,
    ETLStepRunEntity,
    ETLStepsToRun,
    ModelEntity,
    Repeats,
    RunEntity,
    Status,
)
from dbgen.core.model import Model
from dbgen.core.node.extract import Extract
from dbgen.core.node.query import BaseQuery
from dbgen.core.run import RunConfig, RunInitializer, update_run_by_id
from dbgen.exceptions import DBgenExternalError, SerializationError, TransformerError
from dbgen.utils.log import logging_console
from dbgen.utils.typing import NAMESPACE_TYPE, ROWS_TO_LOAD_TYPE

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop

# Initialize multiprocessing start method
set_start_method("spawn")
# Useful types
TRANSFORM_RETURN_TYPE = Union[
    Tuple[None, None, None, int, str], Tuple[list, dict[str, Dict[UUID, dict]], int, int, None]
]

# TODO Add data to the Async Run object to minimize data passing around1
# TODO Refactor the methods to reduce verbosity and increase clarity
# TODO Type annottate queues
# TODO make return types clearer (dataclas?) to reduce verbosity
# TODO update gen_run during the run
class AsyncRun(Base):
    _logger_name: str = 'dbgen.runner'  # type: ignore
    _old_repeats: Set[UUID] = PrivateAttr(default_factory=set)
    _new_repeats: Set[UUID] = PrivateAttr(default_factory=set)
    _run_config: RunConfig = PrivateAttr()

    async def main(
        self,
        etl_step: ETLStep,
        async_dsn: str,
        batch_size: int,
        make_table: Callable[[Progress], Table],
        live: Live,
        run_config: RunConfig,
    ):
        self._run_config = run_config
        conn_pool = AsyncConnectionPool(async_dsn, name='test', min_size=4)
        executor = ProcessPoolExecutor(cpu_count())

        await conn_pool.check()
        loop = asyncio.get_running_loop()
        # Initialize the queues and type them
        transform_queue: asyncio.Queue[Tuple[Optional[UUID], Optional[NAMESPACE_TYPE]]] = asyncio.Queue()
        tform_results: asyncio.Queue[asyncio.Future[TRANSFORM_RETURN_TYPE]] = asyncio.Queue()
        load_queue: asyncio.Queue[Tuple[List[UUID], ROWS_TO_LOAD_TYPE, int]] = asyncio.Queue()
        repeats_queue: asyncio.Queue[Set[UUID]] = asyncio.Queue()

        bars = []
        progress = Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            "[progress.completed]{task.completed:>3.0f} rows",
            refresh_per_second=8,
            transient=True,
        )
        live.update(make_table(progress))
        rows_inserted, rows_loaded = 0, 0
        max_memory = None
        for text in ('Extracted', 'Launched', 'Transformed', 'Loaded'):
            bar = progress.add_task(
                text,
                start=False,
                refresh=False,
                total=100000000,
            )
            bars.append(partial(progress.update, bar))
        try:
            with executor:
                mem_usage = asyncio.create_task(self.memory_usage(executor))
                routines = (
                    self.extractor(
                        etl_step.extract,
                        transform_queue,
                        async_dsn,
                        bar=bars[0],
                        batch_size=batch_size,
                        progress=progress,
                        etl_step_id=etl_step.uuid,
                        retry=run_config.retry,
                    ),
                    self.transformer(
                        etl_step,
                        transform_queue,
                        tform_results,
                        loop,
                        batch_size=batch_size,
                        bar=bars[1],
                        executor=executor,
                    ),
                    self.transformer_results(
                        tform_results,
                        load_queue,
                        bar=bars[2],
                    ),
                    self.loader(etl_step, load_queue, repeats_queue, conn_pool, bars[3], loop=loop),
                    self.repeat_loader(repeats_queue, conn_pool, etl_step.uuid),
                    self.set_length(etl_step.extract, progress, async_dsn),
                )
                tasks = map(asyncio.create_task, routines)
                results = await asyncio.gather(*tasks)
                self._logger.debug('Gathered tasks returned')
                self._logger.debug('Shutting down the executor...')
                mem_usage.cancel()
                max_memory = await mem_usage
            self._logger.debug('Executor shutdown')
        except (Exception, DBgenExternalError) as exc:
            self._logger.error('Uncaught exception found!')
            self._logger.exception(exc, exc_info=exc)
            self._logger.debug('Shutting down tasks')
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._logger.debug('All tasks successfully shutdown')
            return None, None, None, None, None, None, None, format_exc(chain=False)
        finally:
            await conn_pool.close()

        inputs_extracted, unique_inputs, inputs_processed = results[0]
        inputs_skipped = results[2]
        rows_inserted, rows_loaded = results[3]
        return (
            inputs_extracted,
            unique_inputs,
            inputs_processed,
            inputs_skipped,
            rows_inserted,
            rows_loaded,
            max_memory,
            None,
        )

    async def extractor(
        self,
        extract: Extract,
        queue: asyncio.Queue[Tuple[Optional[UUID], Optional[NAMESPACE_TYPE]]],
        async_dsn: str,
        bar,
        batch_size,
        progress,
        etl_step_id: UUID,
        retry: bool,
    ) -> Tuple[int, int, int]:
        """Take a query and param and stream the outputs to the queue."""
        logger = self._logger.getChild('extractor')
        unique_inputs, inputs_extracted, inputs_processed = 0, 0, 0
        if not isinstance(extract, BaseQuery):
            for i, row in enumerate(extract.extract()):
                bar(advance=1)
                is_repeat, input_hash = self._check_repeat(row, etl_step_id)
                # increment unique inputs and extracted inputs
                inputs_extracted += 1
                unique_inputs += 1 if not is_repeat else 0
                if not is_repeat or retry:
                    await queue.put((input_hash, {extract.hash: extract.process_row(row)}))
                    inputs_processed += 1
                else:
                    continue
                if i % batch_size == 0:
                    await asyncio.sleep(0.05)
            await queue.put((None, None))
            for task in progress.tasks:
                progress.update(task.id, total=i)
                progress.start_task(task.id)
        else:
            async with await AsyncConnection.connect(async_dsn) as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    result = await cursor.execute(extract.compiled_query, extract.params)
                    i = 0
                    async for row in result:
                        is_repeat, input_hash = self._check_repeat(row, etl_step_id)
                        # increment unique inputs and extracted inputs
                        inputs_extracted += 1
                        unique_inputs += 1 if not is_repeat else 0
                        if not is_repeat or retry:
                            await queue.put((input_hash, {extract.hash: extract.process_row(row)}))
                        else:
                            continue
                        bar(advance=1)
                        if i % batch_size == 0:
                            await asyncio.sleep(0.05)
                        i += 1

                    await queue.put((None, None))
        logger.debug('Extraction Finished')
        return inputs_extracted, unique_inputs, inputs_processed

    async def transformer(
        self,
        etl_step: ETLStep,
        transform_queue: asyncio.Queue,
        transformed_queue: asyncio.Queue,
        loop: 'AbstractEventLoop',
        batch_size: int = 100,
        bar=None,
        executor=None,
    ):
        batch = []
        logger = self._logger.getChild('transformer')
        pending_tasks: Set[asyncio.Future[TRANSFORM_RETURN_TYPE]] = set()
        while True:
            # get row from producer queue
            input_hash, record = await transform_queue.get()
            # add to the batch
            if record is not None:
                batch.append((input_hash, record))
            # if batch is right size launch the transform
            if len(batch) >= batch_size or record is None:
                # Run transform on the cpu_pool for parallelization
                if len(pending_tasks) >= executor._max_workers:
                    logger.debug('waiting for max workers')
                    _, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                task = loop.run_in_executor(executor, etl_step.transform_batch, batch, self._run_config)
                pending_tasks.add(task)  # type: ignore
                # add to currently running tasks
                await transformed_queue.put(task)
                logger.debug(f"Number of items in Transformed Queue: {transformed_queue.qsize()}")

                bar(advance=len(batch))
                batch = []
            # if None is received from extract queue stop the loop
            if record is None:
                await transformed_queue.put(None)
                break
        logger.debug('Launching Finished')

    async def transformer_results(
        self,
        transformed_queue: asyncio.Queue[asyncio.Future[TRANSFORM_RETURN_TYPE]],
        load_queue: asyncio.Queue,
        bar,
        timeout: float = 0.05,
    ):
        task_set: Set[asyncio.Future[TRANSFORM_RETURN_TYPE]] = set()
        logger = self._logger.getChild('results')
        inputs_skipped = 0
        while True:
            logger.debug('waiting for row')
            task = await transformed_queue.get()
            if task is None:
                break
            logger.debug('got row')
            task_set.add(task)
            logger.debug('waiting for tasks to finish')

            done_set, task_set = await asyncio.wait(
                task_set, return_when=asyncio.FIRST_COMPLETED, timeout=timeout
            )
            logger.debug(f'{len(done_set)} tasks finished')
            logger.debug(f'{len(task_set)} tasks pending')
            for task in done_set:
                processed_hashes, rows_to_load, update, skipped, tb = await task
                inputs_skipped += skipped
                if tb is not None:
                    raise TransformerError(tb)
                bar(advance=update)
                await load_queue.put((processed_hashes, rows_to_load, update))
                logger.debug(f"Number of items in Load Queue: {load_queue.qsize()}")
        if task_set:
            logger.debug(f'queue finished waiting for remaining {len(task_set)} task(s) to finish')
            try:
                for task in asyncio.as_completed(task_set):
                    processed_hashes, rows_to_load, update, skipped, tb = await task
                    inputs_skipped += skipped
                    if tb is not None:
                        raise TransformerError(tb)
                    bar(advance=update)
                    await load_queue.put((processed_hashes, rows_to_load, update))
                    logger.debug('pushed Task')
            except asyncio.TimeoutError:
                logger.error('While waiting for final items in queue to be transformed timeout was reached!')
                done_set, task_set = await asyncio.wait(
                    task_set, return_when=asyncio.FIRST_EXCEPTION, timeout=5
                )
                logger.error(f'{len(done_set)} are done!')
                logger.error(f'Cancelling remaining {len(task_set)}')
                for task in task_set:
                    task.cancel()
                await asyncio.wait_for(asyncio.gather(*task_set, return_exceptions=True), timeout=5)
        await load_queue.put((None, None, None))
        logger.debug('Results Finished')
        return inputs_skipped

    @staticmethod
    async def merge_rows(current_rows, new_rows) -> dict:
        for load_hash, rows in new_rows.items():
            updated_rows = current_rows.get(load_hash, {})
            updated_rows.update(rows)
            current_rows[load_hash] = updated_rows
        return current_rows

    def _check_repeat(self, extracted_dict, etl_step_uuid: UUID) -> Tuple[bool, UUID]:
        # Convert Row to a dictionary so we can hash it for repeat-checking
        input_hash = UUID(hasher((etl_step_uuid, extracted_dict), encoders=encoders))
        # If the input_hash has been seen and we don't have retry=True skip row
        is_repeat = input_hash in self._old_repeats or input_hash in self._new_repeats
        return (is_repeat, input_hash)

    async def loader(
        self,
        etl_step: ETLStep,
        load_queue: asyncio.Queue[Tuple[List[UUID], dict, int]],
        repeat_queue: asyncio.Queue[Set[UUID]],
        conn_pool,
        bar,
        loop,
    ):
        rows_inserted = 0
        rows_updated = 0
        logger = self._logger.getChild('loader')
        while True:
            logger.debug('loader waiting for row')
            rows_to_load: ROWS_TO_LOAD_TYPE = {}
            number_of_rows = 0
            number_of_batches = 0
            processed_hashes: Set[UUID] = set()
            while True:
                new_processed_hashes, record, update = await load_queue.get()
                if record is None:
                    break
                number_of_batches += 1
                number_of_rows += update
                processed_hashes = processed_hashes.union(new_processed_hashes)
                await self.merge_rows(rows_to_load, record)
                if load_queue.empty() or number_of_batches > 30:
                    logger.debug('queue is empty moving on')
                    break
            if rows_to_load:
                for load in etl_step.loads:
                    rows = rows_to_load[load.hash]
                    logger.debug(f'Loading into {load}')
                    rows_modified = await load._async_load(rows, conn_pool, etl_step.hash)
                    if load.insert:
                        rows_inserted += rows_modified
                    else:
                        rows_updated += rows_modified
            bar(advance=number_of_rows)
            await repeat_queue.put(processed_hashes)
            if record is None:
                await repeat_queue.put(None)
                break
        logger.debug('Loading Finished')
        return rows_inserted, rows_updated

    async def repeat_loader(
        self, repeat_queue: asyncio.Queue[Set[UUID]], conn_pool: AsyncConnectionPool, etl_step_id: UUID
    ):
        """Load hash of repeated rows into meta-database"""
        logger = self._logger.getChild('repeat_checker')
        while True:
            logger.debug('repeat checker waiting for row')
            repeats: Set[UUID] = set()
            while True:
                new_repeats = await repeat_queue.get()
                if new_repeats is not None:
                    repeats = repeats.union(new_repeats)
                if repeat_queue.empty() or new_repeats is None:
                    break
            logger.debug('getting ready to load repeats')
            if repeats:
                logger.debug(f'Loading {len(repeats)}repeats')
                await self._load_repeats(conn_pool, repeats, etl_step_id)
            if new_repeats is None:
                break
        logger.debug('Repeat Checking Finished')

    async def _load_repeats(
        self, conn_pool: AsyncConnectionPool, repeats: Set[UUID], etl_step_id: UUID
    ) -> None:
        rows = ((etl_step_id, input_hash) for input_hash in repeats - self._old_repeats)
        async with conn_pool.connection() as connection:
            await Repeats._async_quick_load(connection, rows, column_names=["etl_step_id", "input_hash"])
        self._old_repeats = self._old_repeats.union(repeats)

    async def set_length(self, extract: Extract, progress: Progress, async_dsn: str):
        logger = self._logger.getChild('set_length')
        if isinstance(extract, BaseQuery):
            logger.debug('querying for the query length')
            async with await AsyncConnection.connect(async_dsn) as aconn:
                logger.debug('got connection')
                total = await extract._async_length(connection=aconn)

            logger.debug('got query length')
        else:
            total = extract.length()
        if total is not None:
            for task in progress.tasks:
                progress.update(task.id, total=total)
                progress.start_task(task.id)

        return total

    async def memory_usage(self, executor: ProcessPoolExecutor, refresh_per_second: int = 1):
        """Monitor for the memory usage of the async run and the child processes"""
        logger = self._logger.getChild('memory')
        get_memory = lambda pid=None: psutil.Process(pid).memory_info().rss / (1024 * 1024)
        max_memory = 0
        try:
            while True:
                async_memory_usage = get_memory()
                total_memory_usage = async_memory_usage
                pids = []
                if executor._processes:
                    pids = list(executor._processes.keys())
                    total_memory_usage += sum(map(get_memory, pids))
                max_memory = max(total_memory_usage, max_memory)
                logger.debug(
                    f'Memory Usage ({len(pids)} child processes): Main = {async_memory_usage:3.1f} MB, Total = {total_memory_usage:3.1f} MB'
                )
                await asyncio.sleep(1 / refresh_per_second)

        except asyncio.CancelledError:
            logger.info(f'Max memory used {max_memory:3.1f} MB')
            logger.debug('Memory Usage Finished')
        return max_memory

    def start(
        self,
        etl_step: ETLStep,
        async_dsn: str,
        batch_size: int,
        make_table: Callable[[Progress], Table],
        live: Live,
        run_config: RunConfig,
    ):
        batch_size = batch_size or etl_step.batch_size or 1000
        return asyncio.run(
            self.main(
                etl_step,
                async_dsn,
                batch_size=batch_size,
                make_table=make_table,
                live=live,
                run_config=run_config,
            )
        )


class BaseAsyncETLStepRun(AsyncRun):
    """Lightweight wrapper for etl_step that runs the etl_step with multiprocessing and async."""

    def get_gen(self, meta_engine: Engine, *args, **kwargs) -> ETLStep:
        raise NotImplementedError

    def _initialize_gen_run(
        self,
        session: Session,
        etl_step: ETLStep,
        run_id: Optional[int],
        ordering: Optional[int],
    ) -> ETLStepRunEntity:
        # if no run_id is provided create one and mark it as a testing run
        if run_id is None:
            run = RunEntity(status='testing')
            session.add(run)
            session.commit()
            session.refresh(run)
            ordering = 0
            run_id = run.id
        etl_step_row = etl_step._get_etl_step_row()
        session.merge(etl_step_row)
        session.commit()
        query = etl_step.extract.query if isinstance(etl_step.extract, BaseQuery) else ''
        etl_step_run = ETLStepRunEntity(
            run_id=run_id,
            etl_step_id=etl_step_row.id,
            status=Status.initialized,
            ordering=ordering,
            query=query,
        )
        session.add(etl_step_run)
        session.commit()
        session.refresh(etl_step_run)
        return etl_step_run

    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_id: Optional[int],
        run_config: Optional[RunConfig],
        ordering: Optional[int],
        make_table: Callable[[Progress], Table],
        live: Live,
    ):
        # Set default values for run_config if none provided
        if run_config is None:
            run_config = RunConfig()
        # Get the etl_step
        etl_step = self.get_gen(meta_engine)
        # Initialize the etl_step_row in the meta database
        meta_session = Session(meta_engine)

        gen_run = self._initialize_gen_run(
            etl_step=etl_step, session=meta_session, run_id=run_id, ordering=ordering
        )
        # Check if our run config excludes our etl_step
        if not run_config.should_etl_step_run(etl_step):
            self._logger.info(f'Excluding etl_step {etl_step.name!r}')
            gen_run.status = Status.excluded
            meta_session.commit()
            return
        # Start the ETLStep
        self._logger.info(f'Running etl_step {etl_step.name!r}...')
        gen_run.status = Status.running
        meta_session.commit()
        start = time()
        # Query the repeats table for input_hashes that match this etl_step's hash
        self._logger.debug('Fetching repeats')
        self._old_repeats = set(
            meta_session.exec(select(Repeats.input_hash).where(Repeats.etl_step_id == etl_step.uuid)).all()
        )
        batch_size = run_config.batch_size or etl_step.batch_size or 1000
        assert batch_size is None or batch_size > 0, f"Invalid batch size batch_size must be >0: {batch_size}"
        (
            inputs_extracted,
            unique_inputs,
            inputs_processed,
            inputs_skipped,
            rows_inserted,
            rows_updated,
            memory_usage,
            exc,
        ) = self.start(
            etl_step,
            async_dsn=str(main_engine.url),
            batch_size=batch_size,
            make_table=make_table,
            live=live,
            run_config=run_config,
        )
        if exc:
            gen_run.status = Status.failed
            gen_run.error = str(exc)
            self._logger.error(f"Encountered Error running etl_step {etl_step.name}({etl_step.uuid}).")
            run = meta_session.get(RunEntity, run_id)
            assert run
            run.errors = run.errors + 1 if run.errors else 1
            meta_session.commit()
            meta_session.close()
            return 2

        gen_run.status = Status.completed
        gen_run.inputs_extracted = inputs_extracted
        gen_run.unique_inputs = unique_inputs
        gen_run.inputs_skipped = inputs_skipped
        gen_run.inputs_processed = inputs_processed
        gen_run.rows_updated = rows_updated
        gen_run.rows_inserted = rows_inserted
        gen_run.memory_usage = memory_usage
        gen_run.runtime = round(time() - start, 3)
        self._logger.info(
            f"Finished running etl_step {etl_step.name}({etl_step.uuid}) in {gen_run.runtime}(s)."
        )
        self._logger.info(f"Inserted approximately {gen_run.rows_inserted} rows")
        self._logger.info(f"Updated approximately {gen_run.rows_updated} rows")
        meta_session.commit()
        meta_session.close()
        return 0


class AsyncETLStepRun(BaseAsyncETLStepRun):
    etl_step: ETLStep

    def get_gen(self, meta_engine: Engine, *args, **kwargs):
        return self.etl_step


class RemoteAsyncETLStepRun(BaseAsyncETLStepRun):
    etl_step_id: UUID

    def get_gen(self, meta_engine, *args, **kwargs):
        with Session(meta_engine) as sess:
            etl_step_json = sess.exec(
                select(ETLStepEntity.etl_step_json).where(ETLStepEntity.id == self.etl_step_id)
            ).one()
            try:
                etl_step = ETLStep.deserialize(etl_step_json)
            except ModuleNotFoundError as exc:
                import os

                raise SerializationError(
                    f"While deserializing etl_step id {self.etl_step_id} an unknown module was encountered. Are you using custom dbgen objects reachable by your python environment? Make sure any custom extractors or code can be found in your PYTHONPATH environment variable\nError: {exc}\nPYTHONPATH={os.environ.get('PYTHONPATH')}"
                ) from exc
            if etl_step.uuid != self.etl_step_id:
                error = f"Deserialization Failed the etl_step hash has changed for etl_step named {etl_step.name}!\n{etl_step}\n{self.etl_step_id}"
                raise exceptions.SerializationError(error)
        return etl_step


class AsyncModelRun(Base):
    model: Model

    def get_gen_run(self, etl_step: ETLStep) -> BaseAsyncETLStepRun:
        return BaseAsyncETLStepRun(etl_step=etl_step)

    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_config: RunConfig = None,
        nuke: bool = False,
        rerun_failed: bool = False,
    ) -> RunEntity:
        start = time()
        if run_config is None:
            run_config = RunConfig()
        # Sync the Database statew with the model state
        self.model.sync(main_engine, meta_engine, nuke=nuke)

        # If doing last failed run query for gens to run and add to include
        if rerun_failed:
            with meta_engine.connect() as conn:
                result = conn.execute(select(ETLStepsToRun.__table__.c.name))
                for (gen_name,) in result:
                    run_config.include.add(gen_name)

        # Initialize the run
        run_init = RunInitializer()
        run_id = run_init.execute(meta_engine, run_config)
        sorted_etl_steps = self.model._sort_graph()
        # Add etl_steps to metadb
        with Session(meta_engine) as meta_session:
            model_row = self.model._get_model_row()
            model_row.last_run = datetime.now()
            existing_model = meta_session.get(ModelEntity, model_row.id)
            if not existing_model:
                meta_session.merge(model_row)
            else:
                existing_model.last_run = datetime.now()
            meta_session.commit()

        # Apply start and until to exclude etl_steps not between start_idx and until_idx
        if run_config.start or run_config.until:
            gen_names = [gen.name for gen in sorted_etl_steps]
            start_idx = gen_names.index(run_config.start) if run_config.start else 0
            until_idx = gen_names.index(run_config.until) + 1 if run_config.until else len(gen_names)
            # Modify include to only include the gen_names that pass the test
            run_config.include = run_config.include.union(gen_names[start_idx:until_idx])
            print(f"Only running etl_steps: {gen_names[start_idx:until_idx]} due to start/until")
            self._logger.debug(f"Only running etl_steps: {gen_names[start_idx:until_idx]} due to start/until")

        # Setup UI
        overall_progress = Progress(TimeElapsedColumn(), BarColumn(), TextColumn("{task.description}"))
        etl_step_progress = Progress(
            "{task.description}",
            SpinnerColumn(),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            "[progress.completed]{task.completed:>3.0f} rows",
            refresh_per_second=10,
            # transient=True,
        )
        etl_step_bar = overall_progress.add_task('', total=len(sorted_etl_steps))

        def make_table(progress):
            if not run_config.progress_bar:
                return
            progress_table = Table.grid(expand=True)
            progress_table.add_row(
                Panel(overall_progress, title="Overall Progress", border_style="green", padding=(2, 2)),
                Panel(progress, title="[b]ETLStep", border_style="red", padding=(1, 2)),
            )
            return progress_table

        table = make_table(etl_step_progress)
        with Live(table, console=logging_console, refresh_per_second=10, transient=True) as live:
            for i, etl_step in enumerate(sorted_etl_steps):
                overall_progress.update(
                    etl_step_bar,
                    description=f"[bold #AAAAAA]{etl_step.name} ({i}/{len(sorted_etl_steps)})",
                )
                gen_run = self.get_gen_run(etl_step)
                live.update(make_table(etl_step_progress))
                gen_run.execute(
                    main_engine,
                    meta_engine,
                    run_id,
                    run_config,
                    ordering=i,
                    make_table=make_table,
                    live=live,
                )
                overall_progress.update(etl_step_bar, advance=1)
            overall_progress.update(
                etl_step_bar,
                description=f"[bold #AAAAAA]Finished! ({len(sorted_etl_steps)}/{len(sorted_etl_steps)})",
            )
            overall_progress.update(etl_step_bar, advance=1)
        # Complete run
        with Session(meta_engine) as session:
            update_run_by_id(run_id, Status.completed, session)
            run = session.get(RunEntity, run_id)
            assert run
            run.runtime = timedelta(seconds=time() - start)
            session.commit()
            session.refresh(run)
        return run


class RemoteAsyncModelRun(AsyncModelRun):
    def get_gen_run(self, etl_step):
        return RemoteAsyncETLStepRun(etl_step_id=etl_step.uuid)
