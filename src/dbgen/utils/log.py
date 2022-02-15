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
import logging
from enum import Enum
from io import StringIO
from logging import Formatter, Logger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Tuple

from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install

install()

logging_console = Console()


class LogLevel(str, Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'

    def get_log_level(self):
        return getattr(logging, self)


def capture_stdout(func):
    def wrapped(*args, **kwargs):
        logger = logging.getLogger(f"dbgen.pyblock.{func.name}")
        stream = StringIO()
        with contextlib.redirect_stdout(stream):
            output = func(*args, **kwargs)
        stdout = stream.getvalue().strip()
        if stdout:
            logger.debug(stdout)
        return output

    return wrapped


def setup_logger(level: LogLevel = LogLevel.DEBUG, log_to_stdout=True) -> Tuple[Logger, RichHandler]:
    custom_logger = logging.getLogger("dbgen")
    custom_logger.propagate = True
    custom_logger.setLevel(level.get_log_level())
    rich_handler = add_stdout_logger(custom_logger, LogLevel.WARNING)
    return custom_logger, rich_handler


def add_stdout_logger(logger, stdout_level: LogLevel = LogLevel.DEBUG) -> RichHandler:
    rich_handler = RichHandler(level=stdout_level.get_log_level(), markup=True, console=logging_console)
    log_format = r"[magenta]\[%(name)s][/magenta] - %(message)s"
    rich_handler.setFormatter(Formatter(log_format))
    logger.addHandler(rich_handler)
    return rich_handler


def add_file_handler(logger: Logger, level: LogLevel = LogLevel.DEBUG, file_name: Path = None):
    file_handler = RotatingFileHandler(str(file_name))
    log_format = "[%(asctime)s] - %(name)s - %(levelname)s - %(message)s"
    file_handler.setFormatter(Formatter(log_format))
    file_handler.setLevel(level.get_log_level())
    logger.addHandler(file_handler)
    return file_handler
