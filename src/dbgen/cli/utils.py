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

import logging
from pathlib import Path
from types import FunctionType

import typer

from dbgen.cli.styles import LOGO_STYLE
from dbgen.core.model.model import Model
from dbgen.utils.config import DBgenConfigParser

# Errors
ERROR_FORMAT = "Model is not in MODULE:PACKAGE format: {0}"
ERROR_MODULE = "Could not find module:\nModule: {0}\nPackage: {1}\nError: {2}"
ERROR_PACKAGE = "Could not find package within module:\nModule: {0}\nPackage: {1}\nError: {2}"
ERROR_NOT_MODEL = "Import String is not for a DBgen Model: \nImport String: {0}\nClass: {1}"
ERROR_NOT_MODEL_FUNCTION = "Import String is for a function that does not produce a DBgen Model: \nImport String: {0}\nOutput Class: {1}"
ERROR_RUNNING_MODEL_FACT = "Import String is for a function produced an error or required arguments: \nImport String: {0}\nOutput Class: {1}"

logger = logging.getLogger(__name__)

state = {"confirm": True}
# Callback functions for parsing and validating args
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


def file_existence(value: Path):
    if value and value.exists():
        return value
    elif value is not None:
        # No config found warn user
        logger.warning(f"Provided config {value.absolute()} does not exist.")
    return None


def version_callback(value: bool):
    """
    Eagerly print the version LOGO

    Args:
        value (bool): [description]

    Raises:
        typer.Exit: exits after showing version
    """
    if value:
        typer.echo(LOGO_STYLE)
        raise typer.Exit()


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


def print_config(config: DBgenConfigParser) -> None:
    for section in config:
        values = config.getsection(section) or {}
        if section != "DEFAULT":
            typer.echo("")
            typer.echo(typer.style(f"[{section}]", fg=typer.colors.BRIGHT_CYAN))
            typer.echo(
                "\n".join(
                    [
                        typer.style(f"{key}", fg=typer.colors.BRIGHT_MAGENTA) + f" = {value}"
                        for key, value in values.items()
                    ]
                )
            )


version_option = typer.Option(None, "--version", callback=version_callback, is_eager=True)


# Common options
config_option = typer.Option(
    None,
    "--config",
    "-c",
    help="DBgen config file to use for specifying run parameters as well as DB",
    envvar="DBGEN_CONFIG",
    callback=file_existence,
)
