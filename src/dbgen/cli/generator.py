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

from dbgen.configuration import config, get_engines, root_logger
from dbgen.core.metadata import GeneratorEntity, GeneratorRunEntity, RunEntity
from dbgen.core.run import RemoteGeneratorRun, RunConfig
from dbgen.utils.log import LogLevel, add_stdout_logger

if TYPE_CHECKING:
    from sqlalchemy.future import Engine  # pragma: no cover

generator_app = typer.Typer()


def get_runnable_gens(
    all_runs: bool = True, meta_engine: Optional['Engine'] = None
) -> GenType[Tuple[str, datetime, UUID], None, None]:
    statement = (
        select(  # type: ignore
            GeneratorEntity.name,
            func.max(GeneratorRunEntity.created_at),
            GeneratorEntity.id,
        )
        .join_from(GeneratorEntity, GeneratorRunEntity)
        .group_by(GeneratorEntity.name, GeneratorEntity.id)
        .order_by(GeneratorEntity.name, func.max(GeneratorRunEntity.created_at).desc())
    )
    if not all_runs:
        statement = statement.where(
            GeneratorRunEntity.run_id == select(func.max(RunEntity.id)).scalar_subquery()  # type: ignore
        )

    # If we don't have a metaengine grab one
    if not meta_engine:
        _, meta_engine = get_engines(config)

    with Session(meta_engine) as session:
        result = session.exec(statement.distinct())
        yield from result


@generator_app.command('list')
def list_generators(
    all_runs: bool = typer.Option(
        False, '-a', '--all', help='Print all possible generators not just most recent'
    )
):
    """List out the generators from the last run."""
    typer.echo("-" * 77)
    typer.echo(f"{'name':<20} {'last_run':<20} {'generator_id':20}")
    typer.echo("-" * 77)
    for gen_name, last_run, gen_id in get_runnable_gens(all_runs):
        typer.echo(f"{gen_name!r:<20} {last_run.strftime('%m/%d/%Y %H:%M:%S'):<20} {str(gen_id):<20}")


@generator_app.command('run')
def run_generator(
    gen_id: UUID,
    run_id: Optional[int] = typer.Argument(None),
    ordering: Optional[int] = typer.Argument(None),
    retry: bool = typer.Option(False, help="Ignore repeat checking"),
    level: LogLevel = typer.Option(LogLevel.INFO, help="Log level"),
    include: List[str] = typer.Option([], help="Generators to include"),
    exclude: List[str] = typer.Option([], help="Generators to xclude"),
    start: Optional[str] = typer.Option(None, help="Generator to start run at"),
    until: Optional[str] = typer.Option(None, help="Generator to finish run at."),
    batch: Optional[int] = typer.Option(None, help="Batch size for all generators"),
):
    # Retrieve the configured engines for
    main_engine, meta_engine = get_engines(config)
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
    add_stdout_logger(root_logger, stdout_level=level)
    # Validate that the generator id provided is in the GeneratorEntity Table for remote running
    gens = {gen_id: gen_name for gen_name, _, gen_id in get_runnable_gens(meta_engine=meta_engine)}
    if gen_id not in gens:
        raise typer.BadParameter(f"Invalid generator id, no generator found with uuid {gen_id}.")

    # Need to validate the run_id to make sure that run has been initialized (i.e. a run exists with run_id)
    # and that a gen_run has not already been created with this gen_id and run_id
    if run_id is not None:
        with Session(meta_engine) as session:
            existing_gen_run = session.get(GeneratorRunEntity, (gen_id, run_id))
            if existing_gen_run:
                raise typer.BadParameter(
                    f"Found an existing generator_run with (generator_id, run_id) equal to ({gen_id},{run_id})"
                )
            run = session.get(RunEntity, run_id)
            if not run:
                raise typer.BadParameter(
                    f"No Run exists with run_id = {run_id} has the run been initialized?"
                )
    code = RemoteGeneratorRun(generator_id=gen_id).execute(
        main_engine, meta_engine, run_id, run_config, ordering
    )
    if code:
        raise typer.Exit(code=code)
