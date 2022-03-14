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
from abc import abstractmethod
from typing import TYPE_CHECKING, Dict, List, Optional, Set
from uuid import UUID

from pydantic.fields import Field, PrivateAttr
from sqlalchemy.future import Engine
from sqlmodel import Session

from dbgen.core.base import Base
from dbgen.core.dashboard import Dashboard
from dbgen.core.etl_step import ETLStep
from dbgen.core.metadata import ETLStepRunEntity, RunEntity, Status
from dbgen.core.model import Model
from dbgen.utils.log import LogLevel

if TYPE_CHECKING:
    pass


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
    fail_downstream: bool = False
    fast_fail: bool = False
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
    """Initializes a run by syncing the database and getting the run_id."""

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


class BaseETLStepExecutor(Base):
    etl_step: ETLStep
    run_config: RunConfig
    _etl_step_run: ETLStepRunEntity = PrivateAttr()
    _old_repeats: Set[UUID] = PrivateAttr(default_factory=set)
    _new_repeats: Set[UUID] = PrivateAttr(default_factory=set)

    @abstractmethod
    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        meta_session: Session,
        run_id: Optional[int],
        dashboard: Optional[Dashboard],
        etl_step_run: ETLStepRunEntity,
    ) -> int:
        pass
