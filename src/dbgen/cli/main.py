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
import subprocess
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Optional

import typer

import dbgen.cli.styles as styles
from dbgen import __version__
from dbgen.cli.etl_step import etl_step_app
from dbgen.cli.model import model_app
from dbgen.cli.new import new_app
from dbgen.cli.options import chdir_option, config_option
from dbgen.cli.run import run_app
from dbgen.configuration import config, get_connections
from dbgen.utils.misc import which

app = typer.Typer(no_args_is_help=True)
app.add_typer(
    etl_step_app, name='etl-step', help="Commands related to running and monitoring single ETLSteps."
)
app.add_typer(run_app, name='run', help="Run DBgen models and monitor their status.")
app.add_typer(model_app, name='model', help="Validate, serialize and export DBgen models.")
app.add_typer(new_app, name='new', help="Create new DBgen models from templates.")

app.command("version", help="Print the version of dbgen")(lambda: styles.console.print(styles.LOGO_STYLE))


@app.command()
def version(short: bool = typer.Option(False, '-s', '--short', help='Only output the semantic version.')):
    """Print the dbgen version."""
    if short:
        styles.console.print(__version__)
    else:
        styles.console.print(styles.LOGO_STYLE)


@app.command(name="config")
def get_config(
    config_file: Path = config_option,
    show_password: bool = False,
    show_defaults: bool = False,
    out_pth: Optional[Path] = typer.Option(
        None,
        '--out',
        '-o',
        help="Location to write parametrized config",
    ),
    _chdir: Path = chdir_option,
):
    """
    Prints out the configuration of dbgen given an optional config_file or using the envvar DBGEN_CONFIG
    """
    styles.theme_typer_print(styles.LOGO_STYLE)
    # If out_pth provided write the current config to the path provided and return
    if out_pth:
        with open(out_pth, "w") as f:
            f.write(config.display(True, True))

    typer.echo(config.display(show_defaults, show_password))


class DBgenDatabase(str, Enum):
    """enum for setting database to connect to"""

    META = "meta"
    MAIN = "main"


@app.command(name="connect")
def test_conn(
    connect: DBgenDatabase = typer.Argument(DBgenDatabase.MAIN, help="Database to connect the meta or main."),
    config_file: Optional[Path] = config_option,
    test: bool = typer.Option(False, "-t", "--test", help="Test the main and metadb connections"),
    show_password: bool = typer.Option(
        False, "-p", "--show-password", help="Expose password in printed dsn when testing."
    ),
):
    """
    Prints out the configuration of dbgen given an optional config_file or using the envvar DBGEN_CONFIG
    """
    main_conn, meta_conn = get_connections()

    # If connect is chosen connect to the database selected using CLI sql
    if connect and not test:
        # set the engine based on connect string provided
        conn = main_conn if connect == DBgenDatabase.MAIN else meta_conn
        # Test connection to database
        if not conn.test():
            styles.bad_typer_print(f"Cannot connect to {str(connect)} db")
            raise typer.Exit(2)
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
                [exes[0], conn.url(False, True)],
            )
        except subprocess.CalledProcessError as exc:
            styles.bad_typer_print("Error connecting!")
            styles.bad_typer_print(exc)
        # Quit once finished
        raise typer.Exit()

    styles.delimiter()
    for conn, label in zip((main_conn, meta_conn), ("Main", "Meta")):
        styles.good_typer_print(f"Checking {label} DB...")
        new_stdout = StringIO()
        with contextlib.redirect_stdout(new_stdout):
            check = conn.test()
        test_output = "\n".join(new_stdout.getvalue().strip().split("\n")[1:])
        if check:
            styles.good_typer_print(
                f"Connection to {label} DB at {conn.url(not show_password,True)} all good!"
            )
            if test_output:
                styles.good_typer_print(test_output)
        else:
            styles.bad_typer_print(f"Cannot connect to {label} DB at {conn.url(not show_password,True)}!")
            if test_output:
                styles.bad_typer_print("Error Message:")
                styles.bad_typer_print("\n".join(["\t" + line for line in test_output.split("\n")]))
        styles.delimiter()
