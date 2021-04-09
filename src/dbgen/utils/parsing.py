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
from argparse import ArgumentParser
from distutils.util import strtobool
from re import DOTALL, MULTILINE, finditer, search
from typing import Optional, Tuple


################################################################################
def parse_line(string: str, substr: str, index: int = 0) -> Optional[str]:
    """
    Returns the n'th line containing substring
    Any negative index will return last one.
    """
    iter = finditer(substr + ".*$", string, MULTILINE)
    found = False

    for match in iter:
        if index == 0:
            return match[0]
        else:
            index -= 1
            found = True
    if found:
        return match[0]  # negative input for index
    else:
        return None


def btw(s: str, begin: str, end: str, off: int = 0) -> Tuple[str, int]:
    result = search(f"{begin}(.*?){end}", s[off:], DOTALL)
    if result:
        if result.group(1) is None:
            raise ValueError(f"No Match: {s}")
        return result.group(1), result.end() + off
    else:
        return "", 0


def input_to_level(logging_level: str) -> int:
    """
    Convert logging level string to logging.LEVEL int.

    Args:
        logging_level (str): Logging level string (INFO/DEBUG/WARNING/CRITICAL)

    Raises:
        ValueError: when input string is not a valid logging level

    Returns:
        int: Gets the valid integer from the logging module
    """
    log_map = {
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "WARNING": logging.WARNING,
        "CRITICAL": logging.CRITICAL,
    }
    if logging_level in log_map:
        return log_map[logging_level]
    raise ValueError("Please provide a valid logging level")


########
# Command line parsing
parser = ArgumentParser(description="Run a DBG update", allow_abbrev=True)

parser.add_argument(
    "--nuke",
    default="",
    type=str,
    help="Reset the DB - needed if you make schema changes",
)

parser.add_argument(
    "--add",
    action="store_true",
    help="Try to add columns (if you make benign schema additions)",
)

parser.add_argument("--only", default="", help="Run only the (space separated) generators/tags")

parser.add_argument(
    "--xclude",
    type=str,
    default="",
    help="Run only the (space separated) generators/tags",
)

parser.add_argument("--start", default="", help="Start at the designed Generator")

parser.add_argument("--until", default="", help="Stop at the designed Generator")

parser.add_argument(
    "--retry",
    action="store_true",
    help="Ignore repeat checking",
)

parser.add_argument(
    "--serial",
    default=False,
    type=lambda x: bool(strtobool(x)),
    help='Ignore any "parallel" flags',
)

parser.add_argument(
    "--clean",
    default=False,
    help="Clean the database of the deleted column!DOESN't DELETE DELETED ROWS YET!",
)

parser.add_argument(
    "--skip-row-count",
    action="store_true",
    help="Skip Row count for large queries",
)

parser.add_argument(
    "--batch",
    default=None,
    type=lambda x: int(float(x)),
    help="Set default batch_size",
)

parser.add_argument(
    "--write-logs",
    action="store_true",
    help="write logs to local file",
)

parser.add_argument(
    "--log-level",
    default=logging.DEBUG,
    type=input_to_level,
    help="Set default batch_size",
)

parser.add_argument(
    "--log-path",
    default=None,
    help="Set path of output log file",
)
