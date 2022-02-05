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

"""Objects related to the running of Models and Generators."""
from bdb import BdbQuit
from datetime import datetime, timedelta
from math import ceil
from time import time
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple
from uuid import UUID

from psycopg import connect as pg3_connect
from pydantic.fields import Field, PrivateAttr
from pydasher import hasher
from sqlalchemy.future import Engine
from sqlmodel import Session, select
from tqdm import tqdm

import dbgen.exceptions as exceptions
from dbgen.core.base import Base, encoders
from dbgen.core.generator import Generator
from dbgen.core.metadata import (
    GeneratorEntity,
    GeneratorRunEntity,
    GensToRun,
    ModelEntity,
    Repeats,
    RunEntity,
    Status,
)
from dbgen.core.model import Model
from dbgen.core.node.extract import Extract
from dbgen.core.node.query import BaseQuery
from dbgen.exceptions import (
    DBgenExternalError,
    DBgenSkipException,
    RepeatException,
    SerializationError,
    ValidationError,
)
from dbgen.utils.log import LogLevel


class RunConfig(Base):
    """Configuration for the running of a Generator and Model"""

    retry: bool = False
    include: Set[str] = Field(default_factory=set)
    exclude: Set[str] = Field(default_factory=set)
    start: Optional[str]
    until: Optional[str]
    batch_size: Optional[int]
    progress_bar: bool = True
    skip_row_count: bool = False
    skip_on_error: bool = False
    batch_number: int = 10
    log_level: LogLevel = LogLevel.INFO

    def should_gen_run(self, generator: Generator) -> bool:
        """Check a generator against include/exclude to see if it should run."""
        markers = [generator.name, *generator.tags]
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
        gen_names = model.gens().keys()
        # Validate start and until
        for attr in ("start", "until"):
            val: str = getattr(self, attr)
            if val is not None and val not in gen_names:
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
        # Store the details of the run on the metadatabase so downstream GeneratorRuns can pick them up
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


class BaseGeneratorRun(Base):
    """A lightwieght wrapper for the Generator that grabs a specific Generator from metadatabase and runs it."""

    _old_repeats: Set[UUID] = PrivateAttr(default_factory=set)
    _new_repeats: Set[UUID] = PrivateAttr(default_factory=set)

    def get_gen(self, meta_engine: Engine, *args, **kwargs) -> Generator:
        raise NotImplementedError

    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_id: Optional[int],
        run_config: Optional[RunConfig],
        ordering: Optional[int],
    ):

        # Set default values for run_config if none provided
        if run_config is None:
            run_config = RunConfig()

        generator = self.get_gen(meta_engine=meta_engine)
        # Initialize the generator_row in the meta database
        meta_session = Session(meta_engine)

        gen_run = self._initialize_gen_run(
            generator=generator, session=meta_session, run_id=run_id, ordering=ordering
        )
        # Check if our run config excludes our generator
        if not run_config.should_gen_run(generator):
            self._logger.info(f'Excluding generator {generator.name!r}')
            gen_run.status = Status.excluded
            meta_session.commit()
            return
        # Start the Generator
        self._logger.info(f'Running generator {generator.name!r}...')
        gen_run.status = Status.running
        meta_session.commit()
        start = time()
        # Set the extractor
        self._logger.debug('Initializing extractor')
        extractor_connection = main_engine.connect()
        extract = generator.extract
        if isinstance(extract, BaseQuery):
            extract.set_extractor(connection=extractor_connection, yield_per=generator.batch_size)
        else:
            extract.set_extractor()
        self._logger.debug('Fetching extractor length')
        if not run_config.skip_row_count:
            try:
                row_count = extract.length(connection=extractor_connection)
            except TypeError:
                self._logger.error(
                    f'Extract {extract}\'s length method does not accept the required kwargs, please add **_ kwarg to suppress this error.'
                )
                row_count = None
        else:
            row_count = None

        gen_run.inputs_extracted = row_count
        meta_session.commit()

        self._logger.debug('Fetching repeats')
        # Query the repeats table for input_hashes that match this generator's hash
        self._old_repeats = set(
            meta_session.exec(select(Repeats.input_hash).where(Repeats.generator_id == generator.uuid)).all()
        )
        # The batch_size is set either on the run_config or the generator
        batch_size = run_config.batch_size or generator.batch_size
        if batch_size is None and row_count:
            batch_size = ceil(row_count / run_config.batch_number)
        assert batch_size is None or batch_size > 0, f"Invalid batch size batch_size must be >0: {batch_size}"
        # Open raw connections for fast loading
        main_raw_connection = pg3_connect(str(main_engine.url))
        meta_raw_connection = meta_engine.raw_connection()
        batch_done = lambda x: (x and x % batch_size == 0) if batch_size is not None else False
        # Start while loop to iterate through the nodes
        self._logger.info('Looping through extracted rows...')
        progress_bar = tqdm(
            total=row_count,
            position=1,
            leave=False,
            unit=" Rows",
            disable=not run_config.progress_bar,
        )
        batch_num = 1
        total_batches = f" of {str(ceil(row_count / batch_size))}" if row_count and batch_size else ''
        batch_message = "Batch {}{}: {:10}"
        progress_bar.set_description(batch_message.format(batch_num, total_batches, 'Transforming'))
        try:
            while True:
                row: Dict[str, Mapping[str, Any]] = {}
                try:
                    for node in generator._sort_graph():
                        output = node.run(row)
                        # Extract outputs need to be fed to our repeat checker and need to be checked for stop iterations
                        if isinstance(node, Extract):
                            if output is None or batch_done(gen_run.inputs_processed):
                                load_msg = batch_message.format(batch_num, total_batches, 'Loading')
                                progress_bar.set_description(load_msg)
                                self._logger.debug(load_msg)
                                self._load_repeats(meta_raw_connection, generator)
                                rows_inserted, rows_updated = self._load(main_raw_connection, generator)
                                gen_run.rows_inserted += rows_inserted
                                gen_run.rows_updated += rows_updated
                                meta_session.commit()
                                self._logger.debug('done loading batch.')
                                self._logger.debug(f'inserted {rows_inserted} rows.')
                                self._logger.debug(f'updated {rows_updated} rows.')
                                # Increment batch and move to T-step
                                batch_num += 1
                                transform_msg = batch_message.format(batch_num, total_batches, 'Transforming')
                                progress_bar.set_description(transform_msg)
                                self._logger.debug(transform_msg)
                            # if we are out of rows break out of while loop
                            if output is None:
                                raise StopIteration
                            gen_run.inputs_processed += 1
                            is_repeat, input_hash = self._check_repeat(output, generator.uuid)
                            if not run_config.retry and is_repeat:
                                raise RepeatException()
                        row[node.hash] = output  # type: ignore
                    if not is_repeat:
                        self._new_repeats.add(input_hash)
                        gen_run.unique_inputs += 1
                    progress_bar.update()
                # Stop iteration is used to catch the empty extractor
                except StopIteration:
                    break
                # A repeated input from the extract will also cause a row to be skipped
                except RepeatException:
                    progress_bar.update()
                    continue
                # Any node can raise a skip exception to skip the input before loading
                except DBgenSkipException as exc:
                    self._logger.debug(f"Skipped Row: {exc.msg}")
                    gen_run.inputs_skipped += 1
                    progress_bar.update()
                # External errors are raised whenever a node fails due to internal logic
                except (DBgenExternalError, ValidationError) as e:
                    msg = f"Error when running generator {generator.name}"
                    self._logger.error(msg)
                    self._logger.error(e)
                    if run_config.skip_on_error:
                        gen_run.inputs_skipped += 1
                        progress_bar.update()
                        continue
                    # self._logger.error(f"\n{e}")
                    gen_run.status = Status.failed
                    gen_run.error = str(e)
                    run = meta_session.get(RunEntity, run_id)
                    assert run
                    run.errors = run.errors + 1 if run.errors else 1
                    meta_session.commit()
                    meta_session.close()
                    return 2
                except (
                    Exception,
                    KeyboardInterrupt,
                    SystemExit,
                    BdbQuit,
                ) as e:
                    gen_run.status = Status.failed
                    gen_run.error = (
                        f"Uncaught Error encountered during running of generator {generator.name}: {e!r}"
                    )
                    update_run_by_id(run_id, Status.failed, meta_session)
                    raise
                # finally:
                #     progress_bar.update()
        # Close all connections
        finally:
            gen_run.runtime = round(time() - start, 3)
            meta_session.commit()
            main_raw_connection.close()
            meta_raw_connection.close()
            extractor_connection.close()

        gen_run.status = Status.completed
        gen_run.runtime = round(time() - start, 3)
        self._logger.info(
            f"Finished running generator {generator.name}({generator.uuid}) in {gen_run.runtime}(s)."
        )
        self._logger.info(f"Loaded approximately {gen_run.rows_inserted} rows")
        meta_session.commit()
        meta_session.close()
        return 0

    def _initialize_gen_run(
        self,
        session: Session,
        generator: Generator,
        run_id: Optional[int],
        ordering: Optional[int],
    ) -> GeneratorRunEntity:
        # if no run_id is provided create one and mark it as a testing run
        if run_id is None:
            run = RunEntity(status='testing')
            session.add(run)
            session.commit()
            session.refresh(run)
            ordering = 0
            run_id = run.id
        gen_row = generator._get_gen_row()
        session.merge(gen_row)
        session.commit()
        query = generator.extract.query if isinstance(generator.extract, BaseQuery) else ''
        gen_run = GeneratorRunEntity(
            run_id=run_id,
            generator_id=gen_row.id,
            status=Status.initialized,
            ordering=ordering,
            query=query,
        )
        session.add(gen_run)
        session.commit()
        session.refresh(gen_run)
        return gen_run

    def _load(self, connection, generator: Generator) -> Tuple[int, int]:
        rows_inserted = 0
        rows_updated = 0
        for load in generator._sorted_loads():
            if load.insert:
                rows_inserted += len(load._output)
            else:
                rows_updated += len(load._output)
            load.load(connection, gen_id=self.uuid)
        return (rows_inserted, rows_updated)

    def _load_repeats(self, connection, generator: Generator) -> None:
        rows = ((generator.uuid, input_hash) for input_hash in self._new_repeats)
        Repeats._quick_load(connection, rows, column_names=["generator_id", "input_hash"])
        self._old_repeats = self._old_repeats.union(self._new_repeats)
        self._new_repeats = set()

    def _check_repeat(self, extracted_dict: Dict[str, Any], generator_uuid: UUID) -> Tuple[bool, UUID]:
        # Convert Row to a dictionary so we can hash it for repeat-checking
        input_hash = UUID(hasher((generator_uuid, extracted_dict), encoders=encoders))
        # If the input_hash has been seen and we don't have retry=True skip row
        is_repeat = input_hash in self._old_repeats or input_hash in self._new_repeats
        return (is_repeat, input_hash)


class GeneratorRun(BaseGeneratorRun):
    generator: Generator

    def get_gen(self, meta_engine: Engine, *args, **kwargs):
        return self.generator


class RemoteGeneratorRun(BaseGeneratorRun):
    generator_id: UUID

    def get_gen(self, meta_engine, *args, **kwargs):
        with Session(meta_engine) as sess:
            gen_json = sess.exec(
                select(GeneratorEntity.gen_json).where(GeneratorEntity.id == self.generator_id)
            ).one()
            try:
                generator = Generator.deserialize(gen_json)
            except ModuleNotFoundError as exc:
                import os

                raise SerializationError(
                    f"While deserializing generator id {self.generator_id} an unknown module was encountered. Are you using custom dbgen objects reachable by your python environment? Make sure any custom extractors or code can be found in your PYTHONPATH environment variable\nError: {exc}\nPYTHONPATH={os.environ.get('PYTHONPATH')}"
                ) from exc
            if generator.uuid != self.generator_id:
                error = f"Deserialization Failed the generator hash has changed for generator named {generator.name}!\n{generator}\n{self.generator_id}"
                raise exceptions.SerializationError(error)
        return generator


class ModelRun(Base):
    model: Model

    def get_gen_run(self, generator: Generator) -> BaseGeneratorRun:
        return GeneratorRun(generator=generator)

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
                result = conn.execute(select(GensToRun.__table__.c.name))
                for (gen_name,) in result:
                    run_config.include.add(gen_name)

        # Initialize the run
        run_init = RunInitializer()
        run_id = run_init.execute(meta_engine, run_config)
        sorted_generators = self.model._sort_graph()
        # Add generators to metadb
        with Session(meta_engine) as meta_session:
            model_row = self.model._get_model_row()
            model_row.last_run = datetime.now()
            existing_model = meta_session.get(ModelEntity, model_row.id)
            if not existing_model:
                meta_session.merge(model_row)
            else:
                existing_model.last_run = datetime.now()
            meta_session.commit()

        # Apply start and until to exclude generators not between start_idx and until_idx
        if run_config.start or run_config.until:
            gen_names = [gen.name for gen in sorted_generators]
            start_idx = gen_names.index(run_config.start) if run_config.start else 0
            until_idx = gen_names.index(run_config.until) + 1 if run_config.until else len(gen_names)
            # Modify include to only include the gen_names that pass the test
            run_config.include = run_config.include.union(gen_names[start_idx:until_idx])
            print(f"Only running generators: {gen_names[start_idx:until_idx]} due to start/until")
            self._logger.debug(
                f"Only running generators: {gen_names[start_idx:until_idx]} due to start/until"
            )
        with tqdm(
            total=len(sorted_generators),
            position=0,
            disable=not run_config.progress_bar,
            unit=' Generator',
        ) as tq:
            for i, generator in enumerate(sorted_generators):
                tq.set_description(generator.name)
                gen_run = self.get_gen_run(generator)
                gen_run.execute(main_engine, meta_engine, run_id, run_config, ordering=i)
                tq.update()

        # Complete run
        with Session(meta_engine) as session:
            update_run_by_id(run_id, Status.completed, session)
            run = session.get(RunEntity, run_id)
            assert run
            run.runtime = timedelta(seconds=time() - start)
            session.commit()
            session.refresh(run)
        return run


class RemoteModelRun(ModelRun):
    def get_gen_run(self, generator):
        return RemoteGeneratorRun(generator_id=generator.uuid)
