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

from datetime import datetime
from typing import TYPE_CHECKING
from typing import Generator as GenType
from typing import List, Optional, Tuple
from uuid import UUID

import typer
from sqlmodel import Session, func, select

from dbgen.configuration import get_engines, stdout_handler
from dbgen.core.metadata import ETLStepEntity, ETLStepRunEntity, RunEntity
from dbgen.core.run import RemoteETLStepRun, RunConfig
from dbgen.utils.log import LogLevel

if TYPE_CHECKING:
    from sqlalchemy.future import Engine  # pragma: no cover

etl_step_app = typer.Typer()


def get_runnable_etl_steps(
    all_runs: bool = True, meta_engine: Optional['Engine'] = None
) -> GenType[Tuple[str, datetime, UUID], None, None]:
    statement = (
        select(  # type: ignore
            ETLStepEntity.name,
            func.max(ETLStepRunEntity.created_at),
            ETLStepEntity.id,
        )
        .join_from(ETLStepEntity, ETLStepRunEntity)
        .group_by(ETLStepEntity.name, ETLStepEntity.id)
        .order_by(ETLStepEntity.name, func.max(ETLStepRunEntity.created_at).desc())
    )
    if not all_runs:
        statement = statement.where(
            ETLStepRunEntity.run_id == select(func.max(RunEntity.id)).scalar_subquery()  # type: ignore
        )

    # If we don't have a metaengine grab one
    if not meta_engine:
        _, meta_engine = get_engines()

    with Session(meta_engine) as session:
        result = session.exec(statement.distinct())
        yield from result


@etl_step_app.command('list')
def list_etl_steps(
    all_runs: bool = typer.Option(
        False, '-a', '--all', help='Print all possible ETLSteps not just most recent'
    )
):
    """List out the ETLSteps from the last run."""
    typer.echo("-" * 77)
    typer.echo(f"{'name':<20} {'last_run':<20} {'etl_step_id':20}")
    typer.echo("-" * 77)
    for etl_step_name, last_run, etl_step_id in get_runnable_etl_steps(all_runs):
        typer.echo(
            f"{etl_step_name!r:<20} {last_run.strftime('%m/%d/%Y %H:%M:%S'):<20} {str(etl_step_id):<20}"
        )


@etl_step_app.command('run')
def run_etl_step(
    etl_step_id: UUID,
    run_id: Optional[int] = typer.Argument(None),
    ordering: Optional[int] = typer.Argument(None),
    retry: bool = typer.Option(False, help="Ignore repeat checking"),
    level: LogLevel = typer.Option(LogLevel.INFO, help="Log level"),
    include: List[str] = typer.Option([], help="ETLSteps to include"),
    exclude: List[str] = typer.Option([], help="ETLSteps to xclude"),
    start: Optional[str] = typer.Option(None, help="ETLStep to start run at"),
    until: Optional[str] = typer.Option(None, help="ETLStep to finish run at."),
    batch: Optional[int] = typer.Option(None, help="Batch size for all etl_steps"),
):
    # Retrieve the configured engines for
    main_engine, meta_engine = get_engines()
    run_config = RunConfig(
        retry=retry,
        start=start,
        until=until,
        exclude=exclude,
        include=include,
        progress_bar=False,
        batch_size=batch,
        log_level=level,
    )
    stdout_handler.setLevel(level.get_log_level())
    # Validate that the etl_step id provided is in the ETLStepEntity Table for remote running
    etl_steps = {
        etl_step_id: etl_step_name
        for etl_step_name, _, etl_step_id in get_runnable_etl_steps(meta_engine=meta_engine)
    }
    if etl_step_id not in etl_steps:
        raise typer.BadParameter(f"Invalid etl_step id, no etl_step found with uuid {etl_step_id}.")

    # Need to validate the run_id to make sure that run has been initialized (i.e. a run exists with run_id)
    # and that a etl_step_run has not already been created with this etl_step_id and run_id
    if run_id is not None:
        with Session(meta_engine) as session:
            existing_etl_step_run = session.get(ETLStepRunEntity, (etl_step_id, run_id))
            if existing_etl_step_run:
                raise typer.BadParameter(
                    f"Found an existing etl_step_run with (etl_step_id, run_id) equal to ({etl_step_id},{run_id})"
                )
            run = session.get(RunEntity, run_id)
            if not run:
                raise typer.BadParameter(
                    f"No Run exists with run_id = {run_id} has the run been initialized?"
                )
    code = RemoteETLStepRun(etl_step_id=etl_step_id).execute(
        main_engine, meta_engine, run_id, run_config, ordering
    )
    if code:
        raise typer.Exit(code=code)
