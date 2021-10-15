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
from time import time
from typing import Dict, List, Optional, Set
from uuid import UUID

from pydantic.fields import Field
from pydasher import hasher
from sqlalchemy.future import Engine
from sqlmodel import Session, select
from tqdm import tqdm

from dbgen.core.base import Base, encoders
from dbgen.core.extract import Extract
from dbgen.core.generator import Generator
from dbgen.core.metadata import GeneratorEntity, GeneratorRunEntity, GensToRun, Repeats, RunEntity, Status
from dbgen.core.model import Model
from dbgen.core.query import BaseQuery
from dbgen.exceptions import DBgenExternalError, DBgenSkipException


class RunConfig(Base):
    """Configuration for the running of a Generator and Model"""

    retry: bool = False
    include: Set[str] = Field(default_factory=set)
    exclude: Set[str] = Field(default_factory=set)
    start: Optional[str]
    until: Optional[str]
    batch_size: Optional[int]

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
            gen_run.status = Status.excluded
            meta_session.commit()
            return
        # Start the Generator
        gen_run.status = Status.running
        meta_session.commit()
        start = time()

        # Run the extractor and parse its values
        extractor_connection = main_engine.connect()
        extract = generator.extract
        if isinstance(extract, BaseQuery):
            extractor = extract.extract(connection=extractor_connection)
        else:
            extractor = extract.extract()

        row_count = extract.get_row_count(connection=extractor_connection)
        gen_run.number_of_extracted_rows = row_count
        meta_session.commit()

        # Query the repeats table for input_hashes that match this generator's hash
        old_repeats = set(
            meta_session.exec(select(Repeats.input_hash).where(Repeats.generator_id == generator.uuid)).all()
        )
        # Set up a set of new hashes so we can check for repeating rows coming from the extractor
        new_repeats: Set[UUID] = set()
        # The batch_size is set either on the run_config or the generator
        batch_size = run_config.batch_size or generator.batch_size
        batch_curr = 0
        main_raw_connection = main_engine.raw_connection()
        meta_raw_connection = meta_engine.raw_connection()
        try:
            # Iterate through the extractor and run the output rows through the generators computational graph
            for row in tqdm(extractor, total=row_count, position=2, leave=False, desc="Transforming..."):
                # Convert Row to a dictionary so we can hash it for repeat-checking
                hash_dict = (
                    {key: val for key, val in row[extract.hash].items()} if extract.hash in row else {}
                )
                input_hash = UUID(hasher((generator.uuid, hash_dict), encoders=encoders))
                # If the input_hash has been seen and we don't have retry=True skip row
                is_repeat = input_hash in old_repeats or input_hash in new_repeats
                if not run_config.retry and is_repeat:
                    continue
                else:
                    # Add 1 to unique inputs if not repeat
                    gen_run.unique_inputs += 1 if not is_repeat else 0

                try:
                    batch_curr += 1
                    gen_run.number_of_inputs_processed += 1
                    if batch_size and batch_curr > batch_size:
                        self._load(main_raw_connection, generator)
                        self._load_repeats(meta_raw_connection, new_repeats, generator)
                        old_repeats = old_repeats.union(new_repeats)
                        new_repeats = set()
                        batch_curr = 0
                        meta_session.commit()

                    try:
                        for node in generator._sort_graph():
                            if not isinstance(node, Extract):
                                row[node.hash] = node.run(row)
                        if not is_repeat:
                            new_repeats.add(input_hash)
                    except DBgenSkipException as exc:
                        self._logger.debug(f"Skipped Row:\nmessage:{exc.msg}")
                        gen_run.skipped_inputs += 1
                    except DBgenExternalError as e:
                        msg = f"\n\nError when running generator {generator.name}\n"
                        self._logger.error(msg)
                        self._logger.error(f"\n{e}")
                        gen_run.status = Status.failed
                        gen_run.error = str(e)
                        run = meta_session.get(RunEntity, run_id)
                        assert run
                        run.errors = run.errors + 1 if run.errors else 1
                        meta_session.commit()
                        meta_session.close()
                        return
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
                    meta_session.commit()
                    meta_session.close()
                    main_raw_connection.close()
                    meta_raw_connection.close()
                    extractor_connection.close()
                    raise

            self._load(main_raw_connection, generator)
            self._load_repeats(meta_raw_connection, new_repeats, generator)
            gen_run.status = Status.completed
            gen_run.runtime = round(time() - start, 3)
            meta_session.commit()
        # Close all connections
        finally:
            meta_session.close()
            main_raw_connection.close()
            meta_raw_connection.close()
            extractor_connection.close()

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
        gen_run = GeneratorRunEntity(
            run_id=run_id,
            generator_id=gen_row.id,
            status=Status.initialized,
            ordering=ordering,
        )
        session.add(gen_run)
        session.commit()
        session.refresh(gen_run)
        return gen_run

    def _load(self, connection, generator: Generator) -> None:
        for load in generator._sorted_loads():
            load.load(connection, gen_id=self.uuid)

    def _load_repeats(self, connection, repeats: Set[UUID], generator: Generator) -> None:
        rows = ((generator.uuid, input_hash) for input_hash in repeats)
        Repeats._quick_load(connection, rows, column_names=["generator_id", "input_hash"])


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
            generator = Generator.parse_obj(gen_json)
            assert (
                generator.uuid == self.generator_id
            ), f"Deserialization Failed the generator hash has changed for generator named {generator.name}!\n{generator}\n{self.generator_id}"

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
    ) -> None:
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
        with tqdm(total=len(sorted_generators), position=1) as tq:
            for i, generator in enumerate(sorted_generators):
                tq.set_description(generator.name)
                gen_run = self.get_gen_run(generator)
                gen_run.execute(main_engine, meta_engine, run_id, run_config, ordering=i)
                tq.update()

        # Complete run
        with Session(meta_engine) as session:
            update_run_by_id(run_id, Status.completed, session)


class RemoteModelRun(Base):
    def get_gen_run(self, generator):
        return RemoteGeneratorRun(generator_id=generator.uuid)
