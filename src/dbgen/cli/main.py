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

import json
import logging
from dataclasses import asdict, replace
from enum import Enum
from pathlib import Path
from types import FunctionType
from typing import List, Optional

import typer

from dbgen import LOGO
from dbgen.core.model.model import Model
from dbgen.utils import settings
from dbgen.utils.config import RunConfig, config

# Errors
ERROR_FORMAT = "Model is not in MODULE:PACKAGE format: {0}"
ERROR_MODULE = "Could not find module:\nModule: {0}\nPackage: {1}\nError: {2}"
ERROR_PACKAGE = "Could not find package within module:\nModule: {0}\nPackage: {1}\nError: {2}"
ERROR_NOT_MODEL = "Import String is not for a DBgen Model: \nImport String: {0}\nClass: {1}"
ERROR_NOT_MODEL_FUNCTION = "Import String is for a function that does not produce a DBgen Model: \nImport String: {0}\nOutput Class: {1}"
ERROR_RUNNING_MODEL_FACT = "Import String is for a function produced an error or required arguments: \nImport String: {0}\nOutput Class: {1}"

app = typer.Typer(help="DBgen Model Runner")
state = {"confirm": True}
logger = logging.getLogger(__name__)
LOG_MAP = {
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "WARNING": logging.WARNING,
    "CRITICAL": logging.CRITICAL,
}


class LogLevel(str, Enum):
    """enum for setting logging level on CLI"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


def confirm_nuke(value: bool):
    """
    Confirm that user wants to nuke database

    Args:
        value (bool): nuke_value
    """
    if value and state["confirm"]:
        confirm = input("Are you sure you want to nuke the database? (Y/n) ")
        if confirm.lower() != "y":
            exit(1)
    return value


def set_confirm(value: bool):
    """Auto confirm all prompted values"""
    state["confirm"] = not value


def version_callback(value: bool):
    """
    Eagerly print the version LOGO

    Args:
        value (bool): [description]

    Raises:
        typer.Exit: exits after showing version
    """
    if value:
        typer.echo(LOGO)
        raise typer.Exit()


def file_existence(value: Path):
    if value and value.exists():
        return value
    elif value is not None:
        # No config found warn user
        logger.warning(f"Provided config {value.absolute()} does not exist.")
    return None


def validate_model_str(model_str: str) -> Model:
    """
    Validate the user input model import str checking for malformed and invalid inputs.py

    Args:
        model_str (str): CLI/config file input in MODULE:PACKAGE format

    Raises:
        typer.BadParameter: Whenever one of many checks is failed

    Returns:
        Model: the output model after checks are in place
    """
    basic_error = lambda fmt, val: typer.BadParameter(fmt.format(*val))

    # Check for empty string model_strs
    if model_str in (":", ""):
        raise basic_error(ERROR_FORMAT, [model_str])

    split_model = model_str.split(":")
    if len(split_model) != 2:
        raise basic_error(ERROR_FORMAT, [model_str])
    module, package = split_model
    try:
        _temp = __import__(module, globals(), locals(), [package])
        model = getattr(_temp, package)
        if isinstance(model, Model):
            return model
        elif isinstance(model, FunctionType):
            try:
                model = model()
            except TypeError:
                raise basic_error(ERROR_RUNNING_MODEL_FACT, [model_str, type(model).__name__])
            if isinstance(model, Model):
                return model
            raise basic_error(ERROR_NOT_MODEL_FUNCTION, [model_str, type(model).__name__])

        raise basic_error(ERROR_NOT_MODEL, [model_str, type(model).__name__])
    except ModuleNotFoundError as exc:
        if "No module" in str(exc):
            raise basic_error(ERROR_MODULE, [module, package, str(exc)])
        raise basic_error(ERROR_PACKAGE, [module, package, str(exc)])
    except AttributeError as exc:
        raise typer.BadParameter(str(exc))
    return model


@app.command()
def run(
    model_str: str = typer.Argument(
        ...,
        help="An import string in MODULE:PACKAGE format where the package is either a dbgen model variable or a function that produces one",
    ),
    only: List[str] = typer.Option([], help="Generators to include"),
    xclude: List[str] = typer.Option([], help="Generators to xclude"),
    add: bool = typer.Option(None, help="Add the database objects (tables/columns) before run."),
    retry: bool = typer.Option(None, help="Ignore repeat checking"),
    start: Optional[str] = typer.Option(None, help="Generator to start run at"),
    until: Optional[str] = typer.Option(None, help="Generator to finish run at."),
    serial: bool = None,
    config_file: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="DBgen config file to use for specifying run parameters as well as DB",
        envvar="DBGEN_CONFIG",
        callback=file_existence,
    ),
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
    write_logs: bool = typer.Option(None, help="Write the logs for the dbgen to file."),
    log_level_str: LogLevel = typer.Option(
        LogLevel.INFO, "--log-level", "-L", help="Set the level of logging"
    ),
    log_path: Path = typer.Option(
        None,
        help="Location for log file, overrides the default of $HOME/.dbgen/dbgen.log.",
    ),
    print_logo: bool = False,
    version: bool = typer.Option(None, "--version", callback=version_callback, is_eager=True),
) -> int:
    """
    Run a dbgen model from command line.py

    Args:
        model (Model): [description].
        only (List[str], optional): [description].
        xclude (List[str], optional): [description].
        add (bool, optional): [description].
        retry (bool, optional): [description].
        start (str, optional): [description].
        until (str, optional): [description].
        serial (bool, optional): [description].
        config_file (Optional[Path], optional): [description].
        nuke (bool, optional): [description].
        no_conf (bool, optional): [description].
        bar (bool, optional): [description].
        skip_row_count (bool, optional): [description].
        batch (int, optional): [description].
        write_logs (bool, optional): [description].
        log_level_str (LogLevel, optional): [description].
        log_path (str, optional): Location for log file, overrides the default of $HOME/.dbgen/dbgen.log
        print_logo (bool, optional): [description].
        version (bool, optional): [description].

    Returns:
        dict: [description]
    """
    if print_logo:
        print(LOGO)
    # Parse inputs
    run_config = RunConfig()
    prune = lambda d: {k: v for k, v in d.items() if v is not None and k in run_config.fields}
    prune_inputs = prune(vars())

    # Update the config with the cmd line config
    if config_file:
        config.read(config_file)
    # Use the config file to set RunConfig values
    run_config = replace(run_config, **(prune(config.getsection("run")) or {}))
    run_config = replace(run_config, **(prune(config.getsection("logging")) or {}))
    # Use the CLI to override any configs
    run_config = replace(run_config, **prune_inputs)
    # Initialize settings to get the connections
    settings.initialize()
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
    out_pth: Path = typer.Argument(
        Path("./model.json"),
        help="Output path to write the model.json to",
    ),
):
    """
    Serializes the DBgen model into a json
    """
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


if __name__ == "__main__":
    app()
