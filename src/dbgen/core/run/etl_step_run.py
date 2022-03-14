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
from bdb import BdbQuit
from math import ceil
from time import time
from traceback import format_exc
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple
from uuid import UUID

from psycopg import connect as pg3_connect
from pydasher import hasher
from sqlalchemy.future import Engine
from sqlmodel import Session, select

import dbgen.exceptions as exceptions
from dbgen.configuration import config
from dbgen.core.base import Base, encoders
from dbgen.core.dashboard import BarNames, Dashboard
from dbgen.core.etl_step import ETLStep
from dbgen.core.metadata import ETLStepEntity, ETLStepRunEntity, Repeats, RunEntity, Status
from dbgen.core.node.extract import Extract
from dbgen.core.node.query import BaseQuery, ExternalQuery
from dbgen.core.run.async_run import AsyncETLStepExecutor
from dbgen.core.run.utilities import BaseETLStepExecutor, RunConfig, update_run_by_id
from dbgen.exceptions import SerializationError
from dbgen.utils.typing import NAMESPACE_TYPE

if TYPE_CHECKING:
    from psycopg import Connection as PG3Connection


class ETLStepExecutor(BaseETLStepExecutor):
    """Synchronous ETLStep Executor."""

    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        meta_session: Session,
        run_id: Optional[int],
        dashboard: Optional[Dashboard],
        etl_step_run: ETLStepRunEntity,
    ) -> int:
        # Start the ETLStep
        self._logger.info(f'Running ETLStep {self.etl_step.name!r}...')
        self._etl_step_run = etl_step_run
        self._etl_step_run.status = Status.running
        meta_session.commit()
        start = time()
        self._logger.debug('Fetching repeats')
        # Query the repeats table for input_hashes that match this etl_step's hash
        self._old_repeats = set(
            meta_session.exec(
                select(Repeats.input_hash).where(Repeats.etl_step_id == self.etl_step.uuid)
            ).all()
        )

        # Setup the extractor
        self._logger.debug('Initializing extractor')
        extract = self.etl_step.extract
        with main_engine.connect() as extractor_connection:
            try:
                with extract:
                    # Specifically handle Query extracts by passing in the connection to the database to key methods
                    if isinstance(extract, BaseQuery) and not isinstance(extract, ExternalQuery):
                        extract.set_connection(
                            connection=extractor_connection, yield_per=self.etl_step.batch_size
                        )

                    self._logger.debug('Fetching extractor length')
                    row_count = extract.length() if not self.run_config.skip_row_count else None
                    # Commit the row count to the metadatabase
                    self._etl_step_run.inputs_extracted = row_count
                    meta_session.commit()
                    # The batch_size is set either on the run_config or the etl_step
                    batch_size = self.run_config.batch_size or self.etl_step.batch_size
                    if batch_size is None and row_count:
                        batch_size = ceil(row_count / self.run_config.batch_number)
                    elif batch_size is None:
                        batch_size = config.batch_size
                    # Check for invalid batch sizess
                    if batch_size is not None and batch_size < 0:
                        raise ValueError(f"Invalid batch size batch_size must be >0: {batch_size}")

                    # Open raw connections for fast loading
                    main_raw_connection = pg3_connect(str(main_engine.url))
                    meta_raw_connection = pg3_connect(str(meta_engine.url))
                    # Start while loop to iterate through the nodes
                    self._logger.debug('Looping through extracted rows...')
                    if dashboard is not None:
                        dashboard.add_etl_progress_bars(total=row_count)
                    for batch_ind, batch in enumerate(self.batchify(extract, batch_size, dashboard)):
                        (
                            _,
                            rows_to_load,
                            rows_processed,
                            inputs_skipped,
                            exc,
                        ) = self.etl_step.transform_batch(batch, self.run_config)
                        # Check if transforms or loads raised an error
                        if exc:
                            msg = f"Error when running etl_step {self.etl_step.name}"
                            self._logger.error(msg)
                            self._etl_step_run.status = Status.failed
                            self._etl_step_run.error = exc
                            run = meta_session.get(RunEntity, run_id)
                            assert run
                            run.errors = run.errors + 1 if run.errors else 1
                            meta_session.commit()
                            meta_session.close()
                            return 1
                        if dashboard is not None:
                            dashboard.advance_bar(BarNames.TRANSFORMED, advance=len(batch))
                        rows_inserted, rows_updated = self._load_data(
                            rows_to_load, connection=main_raw_connection
                        )
                        if dashboard is not None:
                            dashboard.advance_bar(BarNames.LOADED, advance=rows_processed)
                        self._load_repeats(meta_raw_connection)
                        self._logger.debug(
                            f'Done loading batch {batch_ind}. Inserted {rows_inserted} and updated {rows_updated} rows.'
                        )

                        # Commit changes to db
                        self._etl_step_run.rows_inserted += rows_inserted
                        self._etl_step_run.rows_updated += rows_updated
                        self._etl_step_run.inputs_skipped += inputs_skipped
                        meta_session.commit()

                # Finish the run and commit to DB
                self._etl_step_run.status = Status.completed
                self._etl_step_run.runtime = round(time() - start, 3)
                self._logger.info(
                    f"Finished running etl_step {self.etl_step.name} in {self._etl_step_run.runtime}(s)."
                )
                self._logger.info(f"Loaded approximately {self._etl_step_run.rows_inserted} rows")
                meta_session.commit()
                meta_session.close()
                return 0
            except (Exception, KeyboardInterrupt, SystemExit, BdbQuit) as exc:
                # Need to catch User Cancels and cancel the whole run
                update_run_by_id(run_id, Status.failed, meta_session)
                if isinstance(exc, (KeyboardInterrupt, BdbQuit)):
                    self._logger.info('Shutting down due to keyboardInterrupt')
                else:
                    msg = f"Uncaught exception while running etl_step {self.etl_step.name}"
                    self._logger.error(msg)
                self._etl_step_run.status = Status.failed
                self._etl_step_run.error = format_exc(limit=1)
                run = meta_session.get(RunEntity, run_id)
                assert run
                run.errors = run.errors + 1 if run.errors else 1
                meta_session.commit()
                meta_session.close()
                raise

    def batchify(
        self, extract: Extract, batch_size: int, dashboard: Optional[Dashboard]
    ) -> Generator[List[Tuple[UUID, NAMESPACE_TYPE]], None, None]:
        # initialize the batch and counts
        batch: List[Tuple[UUID, NAMESPACE_TYPE]] = []
        self._etl_step_run.inputs_extracted = 0
        self._etl_step_run.unique_inputs = 0
        # Loop the the rows in the extract function
        for row in extract.extract():
            # Check the hash of the inputs against the metadatabase
            processed_row = extract.process_row(row)
            is_repeat, input_hash = self._check_repeat(processed_row, self.etl_step.uuid)

            # If we are running with --retry redo repeats
            if self.run_config.retry or not is_repeat:
                # Store the hash for newly seen rows for later loading
                self._etl_step_run.inputs_extracted += 1
                if not is_repeat:
                    self._etl_step_run.unique_inputs += 1
                    self._new_repeats.add(input_hash)
                batch.append((input_hash, {extract.hash: processed_row}))
            elif dashboard is not None:
                dashboard.advance_bar(BarNames.EXTRACTED, advance=1)

            # If batch size is reached advance bar and yield
            if len(batch) == batch_size:
                if dashboard is not None:
                    dashboard.advance_bar(BarNames.EXTRACTED, advance=len(batch))
                yield batch
                batch = []

        # Load the remaining rows
        if batch:
            if dashboard is not None:
                dashboard.advance_bar(BarNames.EXTRACTED, advance=len(batch))
            yield batch
        if dashboard is not None:
            dashboard.set_total(total=self._etl_step_run.inputs_extracted)

    def _load_data(self, rows_to_load: Dict[str, Dict[UUID, Any]], connection) -> Tuple[int, int]:
        rows_inserted = 0
        rows_updated = 0
        for load in self.etl_step._sorted_loads():
            self._logger.debug(f'Loading into {load}')
            rows = rows_to_load[load.hash]
            if load.insert:
                rows_inserted += len(rows)
            else:
                rows_updated += len(rows)
            load._load_data(data=rows, connection=connection, etl_step_id=self.etl_step.uuid)
        return (rows_inserted, rows_updated)

    def _load_repeats(self, connection: 'PG3Connection') -> None:
        rows = {input_hash: (self.etl_step.uuid,) for input_hash in self._new_repeats}
        Repeats._quick_load(connection, rows, column_names=["etl_step_id"])
        self._old_repeats = self._old_repeats.union(self._new_repeats)
        self._new_repeats = set()

    def _check_repeat(self, extracted_dict: Dict[str, Any], etl_step_uuid: UUID) -> Tuple[bool, UUID]:
        # Convert Row to a dictionary so we can hash it for repeat-checking
        input_hash = UUID(hasher((etl_step_uuid, extracted_dict), encoders=encoders))
        # If the input_hash has been seen and we don't have retry=True skip row
        is_repeat = input_hash in self._old_repeats or input_hash in self._new_repeats
        return (is_repeat, input_hash)


class BaseETLStepRun(Base):
    """A lightweight wrapper for the ETLStep that grabs a specific ETLStep from metadatabase and runs it."""

    _run_config: RunConfig = RunConfig()

    class Config:
        """Pydantic COnfig"""

        underscore_attrs_are_private = True

    def get_etl_step(self, meta_engine: Engine, *args, **kwargs) -> ETLStep:
        raise NotImplementedError

    def get_executor(self, etl_step, run_config) -> BaseETLStepExecutor:
        raise NotImplementedError

    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_id: Optional[int],
        run_config: Optional[RunConfig],
        ordering: Optional[int],
        dashboard: Optional[Dashboard] = None,
    ):
        # Set default values for run_config if none provided
        if run_config:
            self._run_config = run_config

        etl_step = self.get_etl_step(meta_engine=meta_engine)
        # Initialize the etl_step_row in the meta database
        meta_session = Session(meta_engine)
        etl_step_run = self._initialize_etl_step_run(
            etl_step=etl_step, session=meta_session, run_id=run_id, ordering=ordering
        )
        # Check if our run config excludes our etl_step
        if not self._run_config.should_etl_step_run(etl_step):
            self._logger.info(f'Excluding etl_step {etl_step.name!r}')
            etl_step_run.status = Status.excluded
            meta_session.commit()
            return
        elif etl_step.name in self._run_config.upstream_fail_exclude:
            self._logger.info(f'Excluding etl_step {etl_step.name!r} due to upstream failure')
            etl_step_run.status = Status.upstream_failed
            meta_session.commit()
            return
        # Initialize the ETLStepExecutor
        executor = self.get_executor(etl_step, self._run_config)
        return_code = executor.execute(
            main_engine=main_engine,
            meta_engine=meta_engine,
            meta_session=meta_session,
            run_id=run_id,
            dashboard=dashboard,
            etl_step_run=etl_step_run,
        )
        return return_code

    def _initialize_etl_step_run(
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
        query = etl_step.extract.render_query() if isinstance(etl_step.extract, BaseQuery) else ''
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


class ETLStepRun(BaseETLStepRun):
    etl_step: ETLStep

    def get_etl_step(self, meta_engine: Engine, *args, **kwargs):
        return self.etl_step

    def get_executor(self, etl_step, run_config) -> BaseETLStepExecutor:
        return ETLStepExecutor(etl_step=etl_step, run_config=run_config)


class RemoteETLStepRun(BaseETLStepRun):
    etl_step_id: UUID

    def get_etl_step(self, meta_engine, *args, **kwargs):
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

    def get_executor(self, etl_step, run_config) -> BaseETLStepExecutor:
        return ETLStepExecutor(etl_step=etl_step, run_config=run_config)


class AsyncETLStepRun(BaseETLStepRun):
    etl_step: ETLStep

    def get_etl_step(self, meta_engine: Engine, *args, **kwargs):
        return self.etl_step

    def get_executor(self, etl_step, run_config) -> BaseETLStepExecutor:
        return AsyncETLStepExecutor(etl_step=etl_step, run_config=run_config)


class AsyncRemoteETLStepRun(BaseETLStepRun):
    etl_step_id: UUID

    def get_etl_step(self, meta_engine, *args, **kwargs):
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

    def get_executor(self, etl_step, run_config) -> BaseETLStepExecutor:
        return AsyncETLStepExecutor(etl_step=etl_step, run_config=run_config)
