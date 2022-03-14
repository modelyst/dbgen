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
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from os import cpu_count
from time import time
from traceback import format_exc
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

import psutil
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydasher import hasher
from sqlalchemy.future import Engine
from sqlmodel import Session

from dbgen.core.base import encoders
from dbgen.core.dashboard import BarNames, Dashboard
from dbgen.core.etl_step import ETLStep
from dbgen.core.metadata import ETLStepRunEntity, Repeats, RunEntity, Status
from dbgen.core.node.extract import Extract
from dbgen.core.node.query import BaseQuery
from dbgen.core.run.utilities import BaseETLStepExecutor
from dbgen.exceptions import DBgenExternalError, TransformerError
from dbgen.utils.typing import NAMESPACE_TYPE, ROWS_TO_LOAD_TYPE

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop

# Useful types
TRANSFORM_RETURN_TYPE = Union[
    Tuple[None, None, None, int, str], Tuple[list, Dict[str, Dict[UUID, dict]], int, int, None]
]

# TODO Add data to the Async Run object to minimize data passing around1
# TODO Refactor the methods to reduce verbosity and increase clarity
# TODO Type annottate queues
# TODO make return types clearer (dataclas?) to reduce verbosity
# TODO update gen_run during the run
class AsyncETLStepExecutor(BaseETLStepExecutor):
    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        meta_session: Session,
        run_id: Optional[int],
        dashboard: Optional[Dashboard],
        etl_step_run: ETLStepRunEntity,
    ):
        start = time()
        batch_size = self.run_config.batch_size or self.etl_step.batch_size or 1000
        (
            inputs_extracted,
            unique_inputs,
            inputs_processed,
            inputs_skipped,
            rows_inserted,
            rows_updated,
            memory_usage,
            exc,
        ) = asyncio.run(
            self.main(
                self.etl_step,
                main_dsn=str(main_engine.url),
                meta_dsn=str(meta_engine.url),
                batch_size=batch_size,
                dashboard=dashboard,
            )
        )
        if exc:
            etl_step_run.status = Status.failed
            etl_step_run.error = str(exc)
            self._logger.error(
                f"Encountered Error running etl_step {self.etl_step.name}({self.etl_step.uuid})."
            )
            run = meta_session.get(RunEntity, run_id)
            assert run
            run.errors = run.errors + 1 if run.errors else 1
            meta_session.commit()
            meta_session.close()
            return 1

        etl_step_run.status = Status.completed
        etl_step_run.inputs_extracted = inputs_extracted
        etl_step_run.unique_inputs = unique_inputs
        etl_step_run.inputs_skipped = inputs_skipped
        etl_step_run.inputs_processed = inputs_processed
        etl_step_run.rows_updated = rows_updated
        etl_step_run.rows_inserted = rows_inserted
        etl_step_run.memory_usage = memory_usage
        etl_step_run.runtime = round(time() - start, 3)
        self._logger.info(
            f"Finished running etl_step {self.etl_step.name}({self.etl_step.uuid}) in {etl_step_run.runtime}(s)."
        )
        self._logger.info(f"Inserted approximately {etl_step_run.rows_inserted} rows")
        self._logger.info(f"Updated approximately {etl_step_run.rows_updated} rows")
        meta_session.commit()
        meta_session.close()
        return

    async def main(
        self,
        etl_step: ETLStep,
        main_dsn: str,
        meta_dsn: str,
        batch_size: int,
        dashboard: Optional[Dashboard],
    ):
        # Initialize multiprocessing start method
        conn_pool = AsyncConnectionPool(main_dsn, name='test', min_size=4)
        meta_conn_pool = AsyncConnectionPool(meta_dsn, name='test', min_size=4)

        await conn_pool.check()
        loop = asyncio.get_running_loop()
        # Initialize the queues and type them
        transform_queue: asyncio.Queue[Tuple[Optional[UUID], Optional[NAMESPACE_TYPE]]] = asyncio.Queue()
        tform_results: asyncio.Queue[asyncio.Future[TRANSFORM_RETURN_TYPE]] = asyncio.Queue()
        load_queue: asyncio.Queue[Tuple[List[UUID], ROWS_TO_LOAD_TYPE, int]] = asyncio.Queue()
        repeats_queue: asyncio.Queue[Set[UUID]] = asyncio.Queue()

        rows_inserted, rows_loaded = 0, 0
        max_memory = None
        if dashboard:
            dashboard.add_etl_progress_bars(run_async=True)
        try:
            context = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(cpu_count(), mp_context=context) as executor:
                mem_usage = asyncio.create_task(self.memory_usage(executor))
                routines = (
                    self.extractor(
                        etl_step.extract,
                        transform_queue,
                        main_dsn,
                        batch_size=batch_size,
                        dashboard=dashboard,
                        etl_step_id=etl_step.uuid,
                        retry=self.run_config.retry,
                    ),
                    self.transformer(
                        etl_step,
                        transform_queue,
                        tform_results,
                        loop,
                        batch_size=batch_size,
                        dashboard=dashboard,
                        executor=executor,
                    ),
                    self.transformer_results(
                        tform_results,
                        load_queue,
                        dashboard=dashboard,
                    ),
                    self.loader(etl_step, load_queue, repeats_queue, conn_pool, dashboard=dashboard),
                    self.repeat_loader(repeats_queue, meta_conn_pool, etl_step.uuid),
                    self.set_length(etl_step.extract, dashboard, conn_pool),
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
        queue: 'asyncio.Queue[Tuple[Optional[UUID], Optional[NAMESPACE_TYPE]]]',
        async_dsn: str,
        batch_size,
        dashboard: Optional[Dashboard],
        etl_step_id: UUID,
        retry: bool,
    ) -> Tuple[int, int, int]:
        """Take a query and param and stream the outputs to the queue."""
        logger = self._logger.getChild('extractor')
        unique_inputs, inputs_extracted, inputs_processed = 0, 0, 0
        with extract:
            if not isinstance(extract, BaseQuery):
                for i, row in enumerate(extract.extract()):
                    if dashboard:
                        dashboard.advance_bar(BarNames.EXTRACTED, advance=1)
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
                # Start the bars with the fully extracted total
                if dashboard:
                    dashboard.set_total(i)
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
                            if dashboard:
                                dashboard.advance_bar(BarNames.EXTRACTED, advance=1)
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
        batch_size: int,
        dashboard: Optional[Dashboard],
        executor,
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
                task = loop.run_in_executor(executor, etl_step.transform_batch, batch, self.run_config)
                pending_tasks.add(task)  # type: ignore
                # add to currently running tasks
                await transformed_queue.put(task)
                logger.debug(f"Number of items in Transformed Queue: {transformed_queue.qsize()}")
                if dashboard:
                    dashboard.advance_bar(BarNames.LAUNCHED, advance=len(batch))
                batch = []
            # if None is received from extract queue stop the loop
            if record is None:
                await transformed_queue.put(None)
                break
        logger.debug('Launching Finished')

    async def transformer_results(
        self,
        transformed_queue: 'asyncio.Queue[asyncio.Future[TRANSFORM_RETURN_TYPE]]',
        load_queue: asyncio.Queue,
        dashboard: Optional[Dashboard],
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
                if dashboard and update:
                    dashboard.advance_bar(BarNames.TRANSFORMED, advance=update)
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
                    if dashboard:
                        dashboard.advance_bar(BarNames.TRANSFORMED, advance=update)
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
        load_queue: 'asyncio.Queue[Tuple[List[UUID], dict, int]]',
        repeat_queue: 'asyncio.Queue[Set[UUID]]',
        conn_pool: AsyncConnectionPool,
        dashboard: Optional[Dashboard],
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
                if load_queue.empty() or number_of_batches > 60:
                    logger.debug('queue is empty moving on')
                    break
            if rows_to_load:
                async with conn_pool.connection() as connection:
                    for load in etl_step.loads:
                        rows = rows_to_load[load.hash]
                        logger.debug(f'Loading into {load}')
                        rows_modified = await load._async_load(rows, connection, etl_step.uuid)
                        if load.insert:
                            rows_inserted += rows_modified
                        else:
                            rows_updated += rows_modified
            if dashboard:
                dashboard.advance_bar(BarNames.LOADED, advance=number_of_rows)
            await repeat_queue.put(processed_hashes)
            if record is None:
                await repeat_queue.put(None)
                break
        logger.debug('Loading Finished')
        return rows_inserted, rows_updated

    async def repeat_loader(
        self, repeat_queue: 'asyncio.Queue[Set[UUID]]', conn_pool: AsyncConnectionPool, etl_step_id: UUID
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
        self,
        conn_pool: AsyncConnectionPool,
        repeats: Set[UUID],
        etl_step_id: UUID,
    ) -> None:
        rows = {input_hash: (etl_step_id,) for input_hash in repeats - self._old_repeats}
        async with conn_pool.connection() as connection:
            await Repeats._async_quick_load(connection, rows, column_names=["etl_step_id"])
        self._old_repeats = self._old_repeats.union(repeats)

    async def set_length(
        self, extract: Extract, dashboard: Optional[Dashboard], conn_pool: AsyncConnectionPool
    ):
        logger = self._logger.getChild('set_length')
        if isinstance(extract, BaseQuery):
            logger.debug('querying for the query length')
            async with conn_pool.connection() as aconn:
                logger.debug('got connection')
                total = await extract._async_length(connection=aconn)

            logger.debug('got query length')
        else:
            total = extract.length()
        if total is not None and dashboard:
            dashboard.set_total(total)
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
