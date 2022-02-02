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
from logging import Formatter

from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install

install()
console = Console()


class LogLevel(str, Enum):
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


def get_log_level(log_level: LogLevel):
    return getattr(logging, log_level)


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


def setup_logger(level: LogLevel = LogLevel.DEBUG, log_to_stdout=True):
    custom_logger = logging.getLogger("dbgen")
    custom_logger.propagate = True
    custom_logger.setLevel(get_log_level(level))
    return custom_logger


def add_stdout_logger(logger, stdout_level: LogLevel = LogLevel.DEBUG):
    level_val = get_log_level(stdout_level)
    rich_handler = RichHandler(level=level_val, markup=True, console=console)
    log_format = r"[magenta]\[%(name)s][/magenta] - %(message)s"
    rich_handler.setFormatter(Formatter(log_format))
    logger.addHandler(rich_handler)
