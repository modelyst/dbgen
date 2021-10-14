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
from typing import List, Optional

import typer

import dbgen.cli.styles as styles
from dbgen.cli.options import model_string_option, version_option
from dbgen.cli.utils import confirm_nuke, validate_model_str
from dbgen.configuration import config
from dbgen.core.query import Connection
from dbgen.core.run import RunConfig

app = typer.Typer()


@app.command()
def print_logo():
    typer.echo(styles.LOGO_STYLE)


@app.command("run")
def run_model(
    model_str: str = model_string_option,
    include: List[str] = typer.Option([], help="Generators to include"),
    exclude: List[str] = typer.Option([], help="Generators to xclude"),
    retry: bool = typer.Option(False, help="Ignore repeat checking"),
    start: Optional[str] = typer.Option(None, help="Generator to start run at"),
    until: Optional[str] = typer.Option(None, help="Generator to finish run at."),
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
    model = validate_model_str(model_str)
    run_config = RunConfig(
        retry=retry,
        start=start,
        until=until,
        exclude=exclude,
        include=include,
    )

    # Validate the run configuration parameters
    invalid_run_config_vals = run_config.get_invalid_markers(model)
    if invalid_run_config_vals:
        lines = [f"{name}: {' ,'.join(val)}" for name, val in invalid_run_config_vals.items()]
        error = "\n".join(lines)
        raise typer.BadParameter(f"Invalid run configuration parameters passed:\n{error}")

    # Start connection from config
    connection = Connection.from_uri(config.postgres_dsn, config.postgres_schema)
    engine = connection.get_engine()
    code = model.run(engine, run_config, nuke=nuke, rerun_failed=rerun_failed)

    raise typer.Exit(code=code)


app.command("version")(lambda: typer.echo(styles.LOGO_STYLE))
app.command("config")(lambda: styles.theme_typer_print(config))
