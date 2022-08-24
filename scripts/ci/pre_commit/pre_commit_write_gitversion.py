#!python
#   Copyright 2022 Modelyst LLC
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

"""
Module to check bump version
"""
import logging
import sys
from os.path import join
from pathlib import Path

from dbgen import __version__

logger = logging.getLogger(__name__)
DBGEN_DIR = str(Path(__file__).parent.parent.parent.parent)


def get_git_version(version_: str):
    """
    Writes the current git version to git_version if this is a git repo

    Args:
        version_ (str): the semantic version to prepend to file

    Returns:
        str: the full version with git
    """
    try:
        import git  # type: ignore

        try:
            repo = git.Repo(join(*[DBGEN_DIR, ".git"]))
        except git.NoSuchPathError:
            logger.warning(".git directory not found: Cannot compute the git version")
            return ""
        except git.InvalidGitRepositoryError:
            logger.warning("Invalid .git directory not found: Cannot compute the git version")
            return ""
    except ImportError:
        logger.warning("gitpython not found: Cannot compute the git version.")
        return ""
    if repo:
        sha = repo.head.commit.hexsha
        if repo.is_dirty():
            return f".dev0+{sha}.dirty"
        # commit is clean
        return f".release:{version_}+{sha}"
    return "no_git_version"


def write_version():
    full_version = get_git_version(__version__)
    if full_version:
        with open(join(DBGEN_DIR, "src", "dbgen", "git_version"), "w") as f:
            f.write(full_version + "\n")


if __name__ == '__main__':
    sys.exit(write_version())
