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
from datetime import datetime, timedelta
from math import ceil
from time import time
from traceback import format_exc
from typing import Any, Dict, Generator, List, Optional, Set, Tuple
from uuid import UUID

from psycopg import connect as pg3_connect
from pydantic.fields import Field, PrivateAttr
from pydasher import hasher
from sqlalchemy.future import Engine
from sqlmodel import Session, select

import dbgen.exceptions as exceptions
from dbgen.configuration import config
from dbgen.core.base import Base, encoders
from dbgen.core.dashboard import BarNames, Dashboard
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
from dbgen.core.node.query import BaseQuery, ExternalQuery
from dbgen.exceptions import SerializationError
from dbgen.utils.log import LogLevel, logging_console
from dbgen.utils.typing import NAMESPACE_TYPE


class RunConfig(Base):
    """Configuration for the running of an ETLStep and Model"""

    retry: bool = False
    include: Set[str] = Field(default_factory=set)
    exclude: Set[str] = Field(default_factory=set)
    upstream_fail_exclude: Set[str] = Field(default_factory=set)
    start: Optional[str]
    until: Optional[str]
    batch_size: Optional[int]
    progress_bar: bool = True
    skip_row_count: bool = False
    fail_downstream: bool = True
    skip_on_error: bool = False
    batch_number: int = 10
    log_level: LogLevel = LogLevel.INFO

    def should_etl_step_run(self, etl_step: ETLStep) -> bool:
        """Check an ETLStep against include/exclude to see if it should run."""
        markers = [etl_step.name, *etl_step.tags]
        should_run = any(
            map(
                lambda x: (not self.include or x in self.include) and x not in self.exclude,
                markers,
            )
        )
        return should_run

    def get_invalid_markers(self, model: Model) -> Dict[str, List[str]]:
        """Check that all inputs to RunConfig are meaningful for the model."""
        invalid_marks = {}
        etl_step_names = model.etl_steps_dict().keys()
        # Validate start and until
        for attr in ("start", "until"):
            val: str = getattr(self, attr)
            if val is not None and val not in etl_step_names:
                invalid_marks[attr] = [val]
        # Validate include and exclude as sets
        for attr in ("include", "exclude"):
            set_val: Set[str] = getattr(self, attr)
            invalid_vals = [x for x in set_val if not model.validate_marker(x)]
            if invalid_vals:
                invalid_marks[attr] = invalid_vals

        return invalid_marks


def update_run_by_id(run_id, status: Status, session: Session):
    run = session.get(RunEntity, run_id)
    assert run, f"No run found with id {run_id}"
    run.status = status
    session.commit()


class RunInitializer(Base):
    """Intializes a run by syncing the database and getting the run_id."""

    def execute(self, engine: Engine, run_config: RunConfig) -> int:
        # Use some metadatabase connection to initialize initialize the run
        # Store the details of the run on the metadatabase so downstream ETLStepRuns can pick them up
        # Sync the database with the registries
        with Session(engine) as session:
            run = RunEntity(status=Status.initialized)
            session.add(run)
            session.commit()
            session.refresh(run)
            assert isinstance(run.id, int)
            run.status = Status.running
            session.commit()
            run_id = run.id

        return run_id


class BaseETLStepRun(Base):
    """A lightwieght wrapper for the ETLStep that grabs a specific ETLStep from metadatabase and runs it."""

    _old_repeats: Set[UUID] = PrivateAttr(default_factory=set)
    _new_repeats: Set[UUID] = PrivateAttr(default_factory=set)
    _run_config: RunConfig = RunConfig()
    _etl_step: ETLStep
    _etl_step_run: ETLStepRunEntity

    class Config:
        """Pydantic COnfig"""

        underscore_attrs_are_private = True

    def get_etl_step(self, meta_engine: Engine, *args, **kwargs) -> ETLStep:
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

        self._etl_step = self.get_etl_step(meta_engine=meta_engine)
        # Initialize the etl_step_row in the meta database
        meta_session = Session(meta_engine)
        self._etl_step_run = self._initialize_etl_step_run(
            etl_step=self._etl_step, session=meta_session, run_id=run_id, ordering=ordering
        )
        # Check if our run config excludes our etl_step
        if not self._run_config.should_etl_step_run(self._etl_step):
            self._logger.info(f'Excluding etl_step {self._etl_step.name!r}')
            self._etl_step_run.status = Status.excluded
            meta_session.commit()
            return
        elif self._etl_step.name in self._run_config.upstream_fail_exclude:
            self._logger.info(f'Excluding etl_step {self._etl_step.name!r} due to upstream failure')
            self._etl_step_run.status = Status.upstream_failed
            meta_session.commit()
            return
        # Start the ETLStep
        self._logger.info(f'Running ETLStep {self._etl_step.name!r}...')
        self._etl_step_run.status = Status.running
        meta_session.commit()
        start = time()
        self._logger.debug('Fetching repeats')
        # Query the repeats table for input_hashes that match this etl_step's hash
        self._old_repeats = set(
            meta_session.exec(
                select(Repeats.input_hash).where(Repeats.etl_step_id == self._etl_step.uuid)
            ).all()
        )
        # Setup the extractor
        self._logger.debug('Initializing extractor')
        extractor_connection = main_engine.connect()
        extract = self._etl_step.extract
        try:
            with extract:
                # Specifically handle Query extracts by passing in the connection to the database to key methods
                if isinstance(extract, BaseQuery) and not isinstance(extract, ExternalQuery):
                    extract.set_connection(
                        connection=extractor_connection, yield_per=self._etl_step.batch_size
                    )

                self._logger.debug('Fetching extractor length')
                row_count = extract.length() if not self._run_config.skip_row_count else None
                # Commit the row count to the metadatabase
                self._etl_step_run.inputs_extracted = row_count
                meta_session.commit()
                # The batch_size is set either on the run_config or the etl_step
                batch_size = self._run_config.batch_size or self._etl_step.batch_size
                if batch_size is None and row_count:
                    batch_size = ceil(row_count / self._run_config.batch_number)
                elif batch_size is None:
                    batch_size = config.batch_size
                # Check for invalid batch sizess
                if batch_size is not None and batch_size < 0:
                    raise ValueError(f"Invalid batch size batch_size must be >0: {batch_size}")

                # Open raw connections for fast loading
                main_raw_connection = pg3_connect(str(main_engine.url))
                meta_raw_connection = meta_engine.raw_connection()
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
                    ) = self._etl_step.transform_batch(batch)
                    # Check if transforms or loads raised an error
                    if exc:
                        msg = f"Error when running etl_step {self._etl_step.name}"
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
                f"Finished running etl_step {self._etl_step.name} in {self._etl_step_run.runtime}(s)."
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
                msg = f"Uncaught exception while running etl_step {self._etl_step.name}"
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
            is_repeat, input_hash = self._check_repeat(processed_row, self._etl_step.uuid)

            # If we are running with --retry redo repeats
            if self._run_config.retry or not is_repeat:
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

    def _load_data(self, rows_to_load: Dict[str, Dict[UUID, Any]], connection) -> Tuple[int, int]:
        rows_inserted = 0
        rows_updated = 0
        for load in self._etl_step._sorted_loads():
            self._logger.debug(f'Loading into {load}')
            rows = rows_to_load[load.hash]
            if load.insert:
                rows_inserted += len(rows)
            else:
                rows_updated += len(rows)
            load._load_data(data=rows, connection=connection, etl_step_id=self._etl_step.uuid)
        return (rows_inserted, rows_updated)

    def _load_repeats(self, connection) -> None:
        rows = ((self._etl_step.uuid, input_hash) for input_hash in self._new_repeats)
        Repeats._quick_load(connection, rows, column_names=["etl_step_id", "input_hash"])
        self._old_repeats = self._old_repeats.union(self._new_repeats)
        self._new_repeats = set()

    def _check_repeat(self, extracted_dict: Dict[str, Any], etl_step_uuid: UUID) -> Tuple[bool, UUID]:
        # Convert Row to a dictionary so we can hash it for repeat-checking
        input_hash = UUID(hasher((etl_step_uuid, extracted_dict), encoders=encoders))
        # If the input_hash has been seen and we don't have retry=True skip row
        is_repeat = input_hash in self._old_repeats or input_hash in self._new_repeats
        return (is_repeat, input_hash)


class ETLStepRun(BaseETLStepRun):
    etl_step: ETLStep

    def get_etl_step(self, meta_engine: Engine, *args, **kwargs):
        return self.etl_step


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


class ModelRun(Base):
    model: Model

    def get_etl_step_run(self, etl_step: ETLStep) -> BaseETLStepRun:
        return ETLStepRun(etl_step=etl_step)

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

        # If doing last failed run query for ETLSteps to run and add to include
        if rerun_failed:
            with meta_engine.connect() as conn:
                result = conn.execute(select(ETLStepsToRun.__table__.c.name))
                for (etl_step_name,) in result:
                    run_config.include.add(etl_step_name)

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
            etl_step_names = [etl_step.name for etl_step in sorted_etl_steps]
            start_idx = etl_step_names.index(run_config.start) if run_config.start else 0
            until_idx = (
                etl_step_names.index(run_config.until) + 1 if run_config.until else len(etl_step_names)
            )
            # Modify include to only include the etl_step_names that pass the test
            run_config.include = run_config.include.union(etl_step_names[start_idx:until_idx])
            print(f"Only running etl_steps: {etl_step_names[start_idx:until_idx]} due to start/until")
            self._logger.debug(
                f"Only running etl_steps: {etl_step_names[start_idx:until_idx]} due to start/until"
            )
        with Dashboard(console=logging_console, enable=run_config.progress_bar).show(
            total=len(sorted_etl_steps)
        ) as dashboard:
            for i, etl_step in enumerate(sorted_etl_steps):
                dashboard.set_etl_name(etl_step.name, i)
                etl_step_run = self.get_etl_step_run(etl_step)
                code = etl_step_run.execute(
                    main_engine, meta_engine, run_id, run_config, ordering=i, dashboard=dashboard
                )
                # If we fail run exclude downstream generators from running
                if code == 1 and run_config.fail_downstream:
                    for target in sorted_etl_steps[i + 1 :]:
                        if target._get_dependency().test(etl_step._get_dependency()):
                            self._logger.info(
                                f'Excluding ETLStep {target.name!r} due to failed upstream dependency {etl_step.name!r}'
                            )
                            run_config.upstream_fail_exclude.add(target.name)
                elif code == 2:
                    break
                dashboard.advance_bar(BarNames.OVERALL)
            dashboard.finish()

        # Complete run
        run_status = Status.completed if code != 2 else Status.failed
        with Session(meta_engine) as session:
            update_run_by_id(run_id, run_status, session)
            run = session.get(RunEntity, run_id)
            assert run
            run.runtime = timedelta(seconds=time() - start)
            session.commit()
            session.refresh(run)
        return run


class RemoteModelRun(ModelRun):
    def get_etl_step_run(self, etl_step):
        return RemoteETLStepRun(etl_step_id=etl_step.uuid)
