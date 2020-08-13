"""Functions for parsing a config file for dbgen environmental variables"""
# External Imports
import configparser
from os import environ
from os.path import exists, abspath
import tempfile
from logging import getLogger

# Internal imports
# from .exceptions import DBgenConfigException


class DBgenConfigParser(configparser.ConfigParser):
    """
    Parser for the dbgen section in a .ini formatted config file.

    Args:
        configparser ([type]): [description]
    """

    pass


logger = getLogger("dbgen.config")

config = DBgenConfigParser()
# Root of project
ROOT = __file__.split("/model/")[0]
CONFIG_PATH = environ.get("DBGEN_CONFIG", "")

# Read the path if it exists
if exists(CONFIG_PATH):
    config.read(CONFIG_PATH)
elif CONFIG_PATH is not None and CONFIG_PATH != "":
    logger.debug(f"No Config file found at {abspath(CONFIG_PATH)}")
else:
    logger.debug(
        f"No Config file provided in environemental variable CONFIG_PATH. Using default values for DBgen variables"
    )
# Add constants section if that does not exist
if "dbgen" not in config:
    config.add_section("dbgen")

DEFAULT_ENV = config.get(
    "dbgen", "default_env", fallback=environ.get("DEFAULT_ENV", None)
)
if DEFAULT_ENV is not None:
    DEFAULT_ENV = abspath(DEFAULT_ENV)
DBGEN_TMP = config.get("dbgen", "dbgen_tmp", fallback=tempfile.gettempdir())
MODEL_NAME = config.get("dbgen", "model_name", fallback="model")
