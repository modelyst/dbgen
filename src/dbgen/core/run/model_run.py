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
from datetime import datetime, timedelta
from time import time
from typing import TYPE_CHECKING

from sqlalchemy.future import Engine
from sqlmodel import Session, select

from dbgen.core.base import Base
from dbgen.core.dashboard import BarNames, Dashboard
from dbgen.core.etl_step import ETLStep
from dbgen.core.metadata import ETLStepsToRun, ModelEntity, RunEntity, Status
from dbgen.core.model import Model
from dbgen.core.run.etl_step_run import (
    AsyncETLStepRun,
    AsyncRemoteETLStepRun,
    BaseETLStepRun,
    ETLStepRun,
    RemoteETLStepRun,
)
from dbgen.core.run.utilities import RunConfig, RunInitializer, update_run_by_id
from dbgen.utils.log import logging_console

if TYPE_CHECKING:
    pass


class ModelRun(Base):
    model: Model

    class Config:
        """Pydantic config"""

        copy_on_model_validation = False

    def get_etl_step_run(self, etl_step: ETLStep, run_async: bool, remote: bool) -> BaseETLStepRun:
        if run_async:
            if remote:
                return AsyncRemoteETLStepRun(etl_step_id=etl_step.uuid)
            else:
                return AsyncETLStepRun(etl_step=etl_step)
        else:
            if remote:
                return RemoteETLStepRun(etl_step_id=etl_step.uuid)
            return ETLStepRun(etl_step=etl_step)

    def execute(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_config: RunConfig = None,
        build: bool = False,
        run_async: bool = False,
        remote: bool = False,
        rerun_failed: bool = False,
    ) -> RunEntity:
        start = time()
        if run_config is None:
            run_config = RunConfig()
        # Sync the Database statew with the model state
        self.model.sync(main_engine, meta_engine, build=build)

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
            self._logger.debug(
                f"Only running etl_steps: {etl_step_names[start_idx:until_idx]} due to start/until"
            )
        with Dashboard(console=logging_console, enable=run_config.progress_bar).show(
            total=len(sorted_etl_steps)
        ) as dashboard:
            for i, etl_step in enumerate(sorted_etl_steps):
                dashboard.set_etl_name(etl_step.name, i)
                etl_step_run = self.get_etl_step_run(etl_step, run_async, remote)
                code = etl_step_run.execute(
                    main_engine, meta_engine, run_id, run_config, ordering=i, dashboard=dashboard
                )
                # If we fail run exclude downstream generators from running
                if code == 1 and (run_config.fail_downstream or run_config.fast_fail):
                    if run_config.fast_fail:
                        self._logger.info(
                            f'Excluding all downstream ETLSteps due to the failure of {etl_step.name!r}'
                        )
                    for target in sorted_etl_steps[i + 1 :]:
                        if run_config.fast_fail:
                            run_config.upstream_fail_exclude.add(target.name)
                        elif target._get_dependency().test(etl_step._get_dependency()):
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
