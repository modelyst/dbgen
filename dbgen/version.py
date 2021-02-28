__all__ = ["version"]
from os.path import exists, join, dirname

try:
    import importlib_metadata as metadata  # type: ignore
except ImportError:
    from importlib import metadata

try:
    version = metadata.version("dbgen")
except metadata.PackageNotFoundError:
    import logging

    log = logging.getLogger(__name__)
    log.warning("Package metadata could not be found. Overriding it with version found in setup.py")
    from setup import version

try:
    curr_dir = dirname(__file__)
    git_ver_file = join(curr_dir, "git_version")
    if exists(git_ver_file):
        with open(git_ver_file) as f:
            git_version = f.read().strip()
    else:
        git_version = ""
except FileNotFoundError:
    git_version = ""
del metadata
