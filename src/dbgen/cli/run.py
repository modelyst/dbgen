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
from logging import getLogger
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import typer
from rich.console import Console
from rich.table import Column, Table
from rich.text import Text
from sqlmodel import Session, select

import dbgen.cli.styles as styles
import dbgen.exceptions as exceptions
from dbgen.cli.options import config_option, model_string_option, version_option
from dbgen.cli.queries import get_runs
from dbgen.cli.utils import confirm_nuke, set_confirm, test_connection, validate_model_str
from dbgen.configuration import config, initialize, root_logger
from dbgen.core.metadata import GeneratorRunEntity, ModelEntity, RunEntity
from dbgen.core.run import RunConfig
from dbgen.utils.log import LogLevel, add_stdout_logger

run_app = typer.Typer(name='run')
logger = getLogger(__name__)


@run_app.command('status')
def status(
    run_id: Optional[int] = None,
    last: int = 1,
    all_runs: bool = typer.Option(False, '-a', '--all', help='show all runs ever'),
    error: bool = typer.Option(False, '--error', help='show error for failed runs'),
    query: bool = typer.Option(False, '--query', help='show error for failed runs'),
    statuses: List[str] = typer.Option(
        [], '-s', '--status', help='Filter out only statuses that match input val'
    ),
):
    column_dict = {'error': Column('Error'), 'status': Column('Status', style='green')}
    _, meta_conn = initialize()
    test_connection(meta_conn)
    meta_engine = meta_conn.get_engine()
    filtered_keys = {'generator_id', 'created_at'}
    if not error:
        filtered_keys.add('error')
    if not query:
        filtered_keys.add('query')
    columns = [
        'name',
        *(c.name for c in GeneratorRunEntity.__table__.c if c.name not in filtered_keys),
    ]
    show_cols = {col: col.replace('number_of_', '') for col in columns}
    table = Table(
        *[column_dict.get(col, col) for col in show_cols],
        title='run',
        show_lines=True,
        highlight=True,
        border_style='magenta',
    )
    status_map = {'failed': 'white on red', 'completed': 'green', 'excluded': 'grey'}

    def style(key, col):
        if key == 'status':
            return Text(str(col), style=status_map.get(col))
        elif key == 'memory_usage':
            return f"{col:3.1f} MB" if col else ''
        elif key == 'runtime':
            return f"{col} (s)" if col else 'N/A'
        return str(col)

    for row in get_runs(run_id, meta_engine, all_runs, statuses, last):
        table.add_row(
            *[style(key, row[key]) for (key, col) in show_cols.items()],
        )
    console = Console()
    console.print(table)


@run_app.callback(invoke_without_command=True)
def run_model(
    ctx: typer.Context,
    model_str: str = model_string_option,
    include: List[str] = typer.Option([], help="Generators to include"),
    exclude: List[str] = typer.Option([], help="Generators to xclude"),
    retry: bool = typer.Option(False, help="Ignore repeat checking"),
    start: Optional[str] = typer.Option(None, help="Generator to start run at"),
    until: Optional[str] = typer.Option(None, help="Generator to finish run at."),
    level: LogLevel = typer.Option(LogLevel.INFO, help='Use the RemoteGenerator Runner'),
    bar: bool = typer.Option(True, help="Show progress bar"),
    skip_row_count: bool = typer.Option(False, help="Show progress bar"),
    skip_on_error: bool = typer.Option(False, help="Skip a row in generator on error"),
    batch: Optional[int] = typer.Option(None, help="Batch size for all generators in run."),
    batch_number: int = typer.Option(10, help="Default number of batches per generator."),
    pdb: bool = typer.Option(False, '--pdb', help="Drop into pdb on breakpoints"),
    config_file: Path = config_option,
    remote: bool = typer.Option(True, help='Use the RemoteGenerator Runner'),
    no_conf: bool = typer.Option(
        False,
        "--no-confirm",
        "-y",
        is_eager=True,
        callback=set_confirm,
    ),
    rerun_failed: bool = typer.Option(
        False,
        help="Only rerun the generators that failed or were excluded in the previous run.",
    ),
    nuke: bool = typer.Option(
        None,
        help="Delete the entire db and meta schema.",
        callback=confirm_nuke,
    ),
    version: bool = version_option,
):
    """Run a model."""
    if ctx.invoked_subcommand is not None:
        return
    # Start connection from config
    main_conn, meta_conn = initialize(config_file)
    # Use config model_str if none is provided
    if model_str is None:
        model_str = config.model_str
    # validate the model_str
    model = validate_model_str(model_str)
    styles.delimiter(styles.typer.colors.GREEN)
    styles.good_typer_print(f"Running model {model.name}...")
    styles.delimiter(styles.typer.colors.GREEN)
    run_config = RunConfig(
        retry=retry,
        start=start,
        until=until,
        exclude=exclude,
        include=include,
        progress_bar=bar,
        log_level=level,
        skip_row_count=skip_row_count,
        skip_on_error=skip_on_error,
        batch_size=batch,
        batch_number=batch_number,
    )
    if not bar:
        add_stdout_logger(root_logger, stdout_level=run_config.log_level)
    # Validate the run configuration parameters
    invalid_run_config_vals = run_config.get_invalid_markers(model)
    if invalid_run_config_vals:
        lines = [f"{name}: {' ,'.join(val)}" for name, val in invalid_run_config_vals.items()]
        error = "\n".join(lines)
        raise typer.BadParameter(f"Invalid run configuration parameters passed:\n{error}")

    # Test each connection with simple select command
    logger.debug('testing connections')
    for name, conn in (('main', main_conn), ('meta', meta_conn)):
        logger.debug(f'testing {name} connection...')
        test_connection(conn, name)
    # Grab engine from each connection
    main_engine, meta_engine = main_conn.get_engine(), meta_conn.get_engine()
    # Pass all the arguments to the model run command
    if pdb:
        config.pdb = pdb
    try:
        out_run = model.run(
            main_engine, meta_engine, run_config, nuke=nuke, rerun_failed=rerun_failed, remote=remote
        )
    except exceptions.SerializationError as exc:
        raise typer.BadParameter(
            f"A generator in your model could not be deserialized. Did you use a custom extractor?"
        ) from exc

    # Once run is done talk to the meta database to report to the user how the run went
    with Session(meta_engine) as session:
        run = session.exec(select(RunEntity).where(RunEntity.id == out_run.id)).one()
        failed_gens = [gen_run.generator.name for gen_run in run.generator_runs if gen_run.status == 'failed']
        rows_inserted = sum([gen_run.rows_inserted for gen_run in run.generator_runs])
        rows_updated = sum([gen_run.rows_updated for gen_run in run.generator_runs])

    if run.errors:
        styles.delimiter(styles.typer.colors.RED)
        styles.bad_typer_print(f"Encountered {run.errors} during the run! The following generators failed:")
        styles.bad_typer_print('\n'.join(map(repr, failed_gens)))
    if run.runtime:
        styles.delimiter(styles.typer.colors.GREEN)
        styles.good_typer_print(
            f"Finished Running {len(model.generators)} Generator(s) in {run.runtime.total_seconds():.3f}(s)."
        )
        styles.good_typer_print(
            f"The run attempted to insert {rows_inserted} and update {rows_updated} rows."
        )
    raise typer.Exit(code=0)


@run_app.command('initialize')
def run_initialize(model_id: UUID, config_file: Path = config_option):
    """Initialize a run."""
    # Notify of config file
    if config_file:
        _, meta_conn = initialize(config_file)
    test_connection(meta_conn)
    meta_engine = meta_conn.get_engine()
    typer.echo(config.display())
    with Session(meta_engine) as session:
        model = session.get(ModelEntity, model_id)
        if not model:
            raise ValueError(f"Invalid model id, could not fine model with ID {model_id}")
        model.last_run = datetime.now()
        run = RunEntity(status='initialized')
        session.add(run)
        session.commit()
        session.refresh(run)
    typer.echo(run.id)
    return
