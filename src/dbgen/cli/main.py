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

import contextlib
import json
import logging
import subprocess
from dataclasses import asdict, replace
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import List, Optional

import typer

import dbgen.cli.styles as styles
import dbgen.cli.test_gen as test_gen
from dbgen.cli.utils import (
    config_option,
    confirm_nuke,
    print_config,
    set_confirm,
    validate_model_str,
    version_option,
)
from dbgen.core.model.model import Model
from dbgen.utils import settings
from dbgen.utils.config import RunConfig, config
from dbgen.utils.misc import which

logger = logging.getLogger(__name__)
LOG_MAP = {
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "WARNING": logging.WARNING,
    "CRITICAL": logging.CRITICAL,
}

# Enumerations
class LogLevel(str, Enum):
    """enum for setting logging level on CLI"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class DBgenDatabase(str, Enum):
    """enum for setting database to connect to"""

    META = "meta"
    MAIN = "main"


# Instantiate the typer app
app = typer.Typer(name="dbgen", help="DBgen Model Runner")
app.command(name="test")(test_gen.test)


@app.command()
def run(
    model_str: str = typer.Argument(
        None,
        help="An import string in MODULE:PACKAGE format where the package is either a dbgen model variable or a function that produces one. If not provided use [core][model_str] in config",
    ),
    only: List[str] = typer.Option([], help="Generators to include"),
    xclude: List[str] = typer.Option([], help="Generators to xclude"),
    add: bool = typer.Option(None, help="Add the database objects (tables/columns) before run."),
    retry: bool = typer.Option(None, help="Ignore repeat checking"),
    start: Optional[str] = typer.Option(None, help="Generator to start run at"),
    until: Optional[str] = typer.Option(None, help="Generator to finish run at."),
    serial: bool = None,
    config_file: Optional[Path] = config_option,
    nuke: bool = typer.Option(
        None,
        help="Delete the entire db and meta schema.",
        callback=confirm_nuke,
    ),
    no_conf: bool = typer.Option(
        False,
        "--no-confirm",
        "-y",
        is_eager=True,
        callback=set_confirm,
    ),
    bar: bool = typer.Option(
        None,
        help="Show tqdm progress bar for the run. Best to disable for Airflow Runs",
    ),
    skip_row_count: bool = typer.Option(False, help="Skip the row count"),
    batch: int = typer.Option(None, help="Batch size for the run. Overrides any Gen-level batch size"),
    print_logo: bool = False,
    version: bool = version_option,
) -> int:
    """
    Run a dbgen model from command line.py
    """
    if print_logo:
        typer.echo(styles.LOGO_STYLE)
    # Parse inputs
    run_config = RunConfig()
    prune = lambda d: {k: v for k, v in d.items() if v is not None and k in run_config.fields}
    prune_inputs = prune(vars())
    # Update the config with the cmd line config
    if config_file:
        config.read(config_file)
    # Use the config file to set RunConfig values
    run_config = replace(run_config, **(prune(config.getsection("run")) or {}))
    # Use the CLI to override any configs
    run_config = replace(run_config, **prune_inputs)
    # Initialize settings to get the connections
    settings.initialize()
    model_str = model_str or config.get("core", "model_str")
    model = validate_model_str(model_str)

    # Validate gen and tag related inputs
    all_names = list(only) + list(xclude)
    all_names += [start] if start else []
    all_names += [until] if until else []
    for val in all_names:
        try:
            model._validate_name(val)
        except ValueError as e:
            if val in only:
                param = "only"
            elif val in xclude:
                param = "xclude"
            elif val == start:
                param = "start"
            else:
                param = "until"
            raise typer.BadParameter(f"{param.upper()} - {e}")
    # Process Log-Level
    assert settings.CONN and settings.META_CONN
    model.run(settings.CONN, settings.META_CONN, **asdict(run_config))
    return 0


@app.command()
def serialize(
    model_str: str = typer.Argument(
        ...,
        help="An import string in MODULE:PACKAGE format where the package is either a dbgen model variable or a function that produces one",
    ),
    out_pth: Path = typer.Option(
        Path("./model.json"),
        "-o",
        "--out-pth",
        help="Output path to write the model.json to",
    ),
):
    """
    Serializes the DBgen model into a json
    """
    model_str = model_str or config.get("core", "model_str")
    model = validate_model_str(model_str)
    with open(out_pth, "w") as f:
        f.write(model.toJSON())
    typer.echo(f"Finished! Your model is serialized at {out_pth.absolute()}")
    typer.echo(f"Hash: {model.hash}")


@app.command()
def deserialize(
    model_path: Path = typer.Argument(
        ...,
        help="Location of stored model.json file to read in",
    ),
):
    """
    Reads a model.json to check for model errors
    """
    try:
        model = Model.fromJSON(model_path.read_text())
    except AssertionError:
        raise typer.BadParameter("Invalid json")
    except json.decoder.JSONDecodeError:
        raise typer.BadParameter("Invalid json")
    assert isinstance(model, Model)
    typer.echo(f"Parsed Model: {model}")
    typer.echo(f"Hash: {model.hash}")
    with open("test.json", "w") as f:
        f.write(model.toJSON())


@app.command()
def airflow(
    model_str: str = typer.Argument(
        ...,
        help="An import string in MODULE:PACKAGE format where the package is either a dbgen model variable or a function that produces one",
    ),
    out_pth: Path = typer.Option(
        None,
        help="Output path to write the model.json to",
    ),
):
    """
    Serializes the DBgen model into a json
    """
    model = validate_model_str(model_str)
    if not out_pth:
        out_pth = Path(f"./{model.name}.py")
    with open(out_pth, "w") as f:
        f.write(model.run_airflow())
    typer.echo(f"Finished! Your model is serialized at {out_pth.absolute()}")
    typer.echo(f"Hash: {model.hash}")


@app.command(name="config")
def get_config(
    config_file: Optional[Path] = config_option,
    out_pth: Optional[Path] = typer.Option(
        None,
        help="Location to write parametrized config",
    ),
):
    """
    Prints out the configuration of dbgen given an optional config_file or using the envvar DBGEN_CONFIG
    """
    if config_file:
        config.read(config_file)
    # If out_pth provided write the current config to the path provided and return
    if out_pth:
        with open(out_pth, "w") as f:
            config.write(f)

    typer.echo(styles.LOGO_STYLE)

    # Initialize settings to get the connections
    settings.initialize()

    # Print config to stdout
    styles.delimiter()
    # Notify of config file
    if config_file:
        typer.echo(f"Config File found at location: {config_file.absolute()}")
    else:
        typer.echo("No config file found using defaults.")

    styles.delimiter()
    print_config(config)


@app.command(name="connect")
def test_conn(
    connect: DBgenDatabase = typer.Argument(DBgenDatabase.MAIN, help="Expose password in printed dsn."),
    config_file: Optional[Path] = config_option,
    test: bool = typer.Option(False, "-t", "--test", help="Test the main and metadb connections"),
    with_password: bool = typer.Option(
        False, "-p", "--password", help="Expose password in printed dsn when testing."
    ),
):
    """
    Prints out the configuration of dbgen given an optional config_file or using the envvar DBGEN_CONFIG
    """
    if config_file:
        config.read(config_file)

    settings.initialize()
    assert settings.CONN and settings.META_CONN

    # If connect is chosen connect to the database selected using CLI sql
    if connect and not test:
        # set the connection_info based on connect string provided
        connection_info = settings.CONN if connect == DBgenDatabase.MAIN else settings.META_CONN
        # Test connection to database
        if not connection_info.test_connection():
            styles.bad_typer_print(f"Cannot connect to {connect} db")
            raise typer.Exit(2)
        else:
            # Attempt to use psql and pgcli to connect to database
            try:
                # Filter out executibles using which function
                exes = list(filter(lambda x: x is not None, map(which, ("pgcli", "psql"))))
                # If we find no executables exit
                if not exes:
                    styles.bad_typer_print(
                        "Cannot find either psql or pgcli in $PATH. Please install them to connect to database."
                    )
                    raise typer.Exit(2)
                # If we have valid executible run the command with the dsn provided
                subprocess.check_call(
                    [exes[0], connection_info.to_dsn(with_password=True)],
                )
            except subprocess.CalledProcessError as exc:
                styles.bad_typer_print("Error connecting!")
                styles.bad_typer_print(exc)
        # Quit once finished
        raise typer.Exit()

    styles.delimiter()
    for conn, label in zip((settings.CONN, settings.META_CONN), ("Main", "Meta")):
        styles.good_typer_print(f"Checking {label} DB...")
        new_stdout = StringIO()
        with contextlib.redirect_stdout(new_stdout):
            check = conn.test_connection(with_password)
        test_output = "\n".join(new_stdout.getvalue().strip().split("\n")[1:])
        if check:
            styles.good_typer_print(f"Connection to {label} DB at {conn.to_dsn(with_password)} all good!")
            if test_output:
                styles.good_typer_print(test_output)
        else:
            styles.bad_typer_print(f"Cannot connect to {label} DB at {conn.to_dsn(with_password)}!")
            if test_output:
                styles.bad_typer_print("Error Message:")
                styles.bad_typer_print("\n".join(["\t" + line for line in test_output.split("\n")]))
        styles.delimiter()


if __name__ == "__main__":
    app()
