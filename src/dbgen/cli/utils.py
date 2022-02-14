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
import os
import sys
from pathlib import Path
from types import FunctionType
from typing import TYPE_CHECKING, Dict, Union

import typer

from dbgen.cli.styles import LOGO_STYLE, bad_typer_print
from dbgen.configuration import update_config
from dbgen.core.model import Model

if TYPE_CHECKING:
    from dbgen.utils.sql import Connection  # pragma: no cover

# Errors
ERROR_FORMAT = "Model is not in MODULE:PACKAGE format: {0}"
ERROR_DEFAULT_MODEL = "Could not find model at model.main:make_model.\nThis is the default --model value, did you forget to set --model?"
ERROR_MODULE = "Could not find module:\nModule: {0}\nPackage: {1}\nError: {2}"
ERROR_PACKAGE = "Could not find package within module:\nModule: {0}\nPackage: {1}\nError: {2}"
ERROR_ATTR = "Could not find Attribute within package:\nModule: {0}\nPackage: {1}\nError: {2}"
ERROR_NOT_MODEL = "Import String is not for a DBgen Model: \nImport String: {0}\nClass: {1}"
ERROR_NOT_MODEL_FUNCTION = "Import String is for a function that does not produce a DBgen Model: \nImport String: {0}\nOutput Class: {1}"
ERROR_RUNNING_MODEL_FACT = "Import String is for a function produced an error or required arguments: \nImport String: {0}\nOutput Class: {1} \n{2}{3}"

logger = logging.getLogger(__name__)

state: Dict[str, Union[bool, Path, None]] = {"confirm": True, 'verbose': True, 'old_cwd': None}


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


def set_verbosity(verbose: bool):
    """Auto confirm all prompted values"""
    state["verbose"] = not verbose


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
    # Add current workind directory to the path at the end
    cwd = os.getcwd()
    sys.path.insert(0, cwd)

    basic_error = lambda fmt, val: typer.BadParameter(fmt.format(*val), param_hint='--model')

    if model_str is None:
        raise typer.BadParameter("--model is required.")

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
            except TypeError as exc:
                print(exc)
                import traceback

                exc_str = traceback.format_exc()
                raise basic_error(
                    ERROR_RUNNING_MODEL_FACT, [model_str, type(model).__name__, "#" * 24 + "\n", str(exc_str)]
                ) from exc
            if isinstance(model, Model):
                sys.path.remove(cwd)
                return model
            raise basic_error(ERROR_NOT_MODEL_FUNCTION, [model_str, type(model).__name__])

        raise basic_error(ERROR_NOT_MODEL, [model_str, type(model).__name__])
    except (ModuleNotFoundError, AttributeError) as exc:
        if model_str == 'model.main:make_model':
            raise basic_error(ERROR_DEFAULT_MODEL, []) from exc

        if "No module" in str(exc):
            raise basic_error(ERROR_MODULE, [module, package, str(exc)]) from exc
        if isinstance(exc, AttributeError):
            raise basic_error(ERROR_ATTR, [module, package, str(exc)]) from exc
        raise basic_error(ERROR_PACKAGE, [module, package, str(exc)]) from exc


CONNECT_ERROR = "Cannot connect to database({name!r}) with connection string {url}. You can test your connection with dbgen connect --test"


def test_connection(conn: 'Connection', name: str = ''):
    if not conn.test():
        bad_typer_print(CONNECT_ERROR.format(name='meta', url=conn.url()))
        raise typer.Exit(code=2)


def chdir_callback(directory: Path):
    if directory:
        state['old_cwd'] = directory.cwd()
        os.chdir(directory)


def config_callback(config_file: Path):
    if config_file.exists():
        update_config(config_file)
    elif str(config_file.name) != '.env':
        raise typer.BadParameter(f'Config file \'{config_file}\' does not exist')
    return config_file
