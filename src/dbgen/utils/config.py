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

"""Functions for parsing a config file for dbgen environmental variables"""
import pathlib

# External Imports
import shlex
import subprocess
import sys
import tempfile
from collections import OrderedDict
from configparser import ConfigParser
from dataclasses import dataclass, field, fields
from logging import getLogger
from os import environ
from os.path import dirname, expanduser, expandvars, join
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

# Internal imports
from dbgen.utils.exceptions import DBgenConfigException
from dbgen.utils.module_loading import import_string

logger = getLogger("dbgen.config")


def _get_config_value_from_secret_backend(config_key):
    """Get Config option values from Secret Backend"""
    # !TODO!
    raise NotImplementedError
    secrets_client = None  # get_custom_secret_backend()
    if not secrets_client:
        return None
    return secrets_client.get_config(config_key)


def _read_default_config_file(file_name: str) -> Tuple[str, str]:
    templates_dir = join(dirname(__file__), "config_templates")
    file_path = join(templates_dir, file_name)
    with open(file_path, encoding="utf-8") as config_file:
        return config_file.read(), file_path


DEFAULT_CONFIG, DEFAULT_CONFIG_FILE_PATH = _read_default_config_file("default_dbgen.cfg")


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = expanduser(expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def run_command(command):
    """Runs command and returns stdout"""
    process = subprocess.Popen(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    output, stderr = [stream.decode(sys.getdefaultencoding(), "ignore") for stream in process.communicate()]

    if process.returncode != 0:
        raise DBgenConfigException(
            f"Cannot execute {command}. Error code is: {process.returncode}. "
            f"Output: {output}, Stderr: {stderr}"
        )

    return output


class DBgenConfigParser(ConfigParser):
    """
    Parser for the dbgen section in a .ini formatted config file.

    Args:
        configparser ([type]): [description]
    """

    sensitive_config_values = {
        ("core", "db_conn_str"),
        ("core", "metadb_conn_str"),
        ("logging", "log_path"),
    }

    deprecated_options: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
    ENV_VAR_PREFIX = "DBGEN__"

    def __init__(self, default_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dbgen_defaults = ConfigParser(*args, **kwargs)
        if default_config is not None:
            self.dbgen_defaults.read_string(default_config)

        self.is_validated = False

    def _env_var_name(self, section: str, key: str) -> str:
        return f"{self.ENV_VAR_PREFIX}{section.upper()}__{key.upper()}"

    def _get_env_var_option(self, section, key):
        # must have format DBGEN__{SECTION}__{KEY} (note double underscore)
        env_var = self._env_var_name(section, key)
        if env_var in environ:
            return expand_env_var(environ[env_var])
        # alternatively DBGEN__{SECTION}__{KEY}_CMD (for a command)
        env_var_cmd = env_var + "_CMD"
        if env_var_cmd in environ:
            # if this is a valid command key...
            if (section, key) in self.sensitive_config_values:
                return run_command(environ[env_var_cmd])
        # alternatively DBGEN__{SECTION}__{KEY}_SECRET (to get from Secrets Backend)
        env_var_secret_path = env_var + "_SECRET"
        if env_var_secret_path in environ:
            # if this is a valid secret path...
            if (section, key) in self.sensitive_config_values:
                return _get_config_value_from_secret_backend(environ[env_var_secret_path])
        return None

    def _get_cmd_option(self, section, key):
        fallback_key = key + "_cmd"
        # if this is a valid command key...
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                command = super().get(section, fallback_key)
                return run_command(command)
        return None

    def _get_secret_option(self, section, key):
        """Get Config option values from Secret Backend"""
        fallback_key = key + "_secret"
        # if this is a valid secret key...
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                secrets_path = super().get(section, fallback_key)
                return _get_config_value_from_secret_backend(secrets_path)
        return None

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        deprecated_section, deprecated_key, _ = self.deprecated_options.get(
            (section, key), (None, None, None)
        )

        option = self._get_environment_variables(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        option = self._get_option_from_config_file(deprecated_key, deprecated_section, key, kwargs, section)
        if option is not None:
            return option

        option = self._get_option_from_commands(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        option = self._get_option_from_secrets(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        return self._get_option_from_default_config(section, key, **kwargs)

    def _get_option_from_default_config(self, section, key, **kwargs):
        # ...then the default config
        if self.dbgen_defaults.has_option(section, key) or "fallback" in kwargs:
            return expand_env_var(self.dbgen_defaults.get(section, key, **kwargs))

        else:
            logger.warning("section/key [%s/%s] not found in config", section, key)

            raise DBgenConfigException(f"section/key [{section}/{key}] not found in config")

    def _get_option_from_secrets(self, deprecated_key, deprecated_section, key, section):
        # ...then from secret backends
        option = self._get_secret_option(section, key)
        if option:
            return option
        if deprecated_section:
            option = self._get_secret_option(deprecated_section, deprecated_key)
            if option:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def _get_option_from_commands(self, deprecated_key, deprecated_section, key, section):
        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option
        if deprecated_section:
            option = self._get_cmd_option(deprecated_section, deprecated_key)
            if option:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def _get_option_from_config_file(self, deprecated_key, deprecated_section, key, kwargs, section):
        # ...then the config file
        if super().has_option(section, key):
            # Use the parent's methods to get the actual config here to be able to
            # separate the config from default config.
            return expand_env_var(super().get(section, key, **kwargs))
        if deprecated_section:
            if super().has_option(deprecated_section, deprecated_key):
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return expand_env_var(super().get(deprecated_section, deprecated_key, **kwargs))
        return None

    def _get_environment_variables(self, deprecated_key, deprecated_section, key, section):
        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option
        if deprecated_section:
            option = self._get_env_var_option(deprecated_section, deprecated_key)
            if option is not None:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def getsection(self, section: str) -> Optional[Dict[str, Union[str, int, float, bool]]]:
        """
        Returns the section as a dict. Values are converted to int, float, bool
        as required.

        :param section: section from the config
        :rtype: dict
        """
        if not self.has_section(section) and not self.dbgen_defaults.has_section(section):
            return None

        if self.dbgen_defaults.has_section(section):
            _section = OrderedDict(self.dbgen_defaults.items(section))
        else:
            _section = OrderedDict()

        if self.has_section(section):
            _section.update(OrderedDict(self.items(section)))

        section_prefix = self._env_var_name(section, "")
        for env_var in sorted(environ.keys()):
            if env_var.startswith(section_prefix):
                key = env_var.replace(section_prefix, "")
                if key.endswith("_CMD"):
                    key = key[:-4]
                key = key.lower()
                _section[key] = self._get_env_var_option(section, key)

        for key, val in _section.items():
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    if val.lower() in ("t", "true"):
                        val = True
                    elif val.lower() in ("f", "false"):
                        val = False
            _section[key] = val
        return _section

    def getimport(self, section, key, **kwargs):  # noqa
        """
        Reads options, imports the full qualified name, and returns the object.

        In case of failure, it throws an exception a clear message with the key aad the section names

        :return: The object or None, if the option is empty
        """
        full_qualified_path = self.get(section=section, key=key, **kwargs)
        if not full_qualified_path:
            return None

        try:
            return import_string(full_qualified_path)
        except ImportError as e:
            logger.error(e)
            raise DBgenConfigException(
                f'The object could not be loaded. Please check "{key}" key in "{section}" section. '
                f'Current value: "{full_qualified_path}".'
            )


@dataclass
class RunConfig:
    """Captures all configurations for a run"""

    only: List[str] = field(default_factory=lambda: [])
    xclude: List[str] = field(default_factory=lambda: [])
    add: bool = False
    retry: bool = False
    start: Optional[str] = None
    until: Optional[str] = None
    serial: bool = False
    nuke: bool = False
    bar: bool = True
    skip_row_count: bool = True
    batch: Optional[int] = None

    @property
    def fields(self):
        return set(map(lambda x: x.name, fields(self)))


DEFAULT_CONFIG, DEFAULT_CONFIG_FILE_PATH = _read_default_config_file("default_dbgen.cfg")


def get_dbgen_home():
    """Get path to DBGEN Home"""
    return expand_env_var(environ.get("DBGEN_HOME", "~/.dbgen"))


def get_dbgen_config(dbgen_home: str):
    """Get Path to airflow.cfg path"""
    if "DBGEN_CONFIG" not in environ:
        return join(dbgen_home, "dbgen.cfg")
    return expand_env_var(environ["DBGEN_CONFIG"])


DBGEN_HOME = get_dbgen_home()
DBGEN_CONFIG = get_dbgen_config(DBGEN_HOME)
pathlib.Path(DBGEN_HOME).mkdir(parents=True, exist_ok=True)


def parameterized_config(template):
    """
    Generates a configuration from the provided template + variables defined in
    current scope

    :param template: a config content templated with {{variables}}
    """
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)  # noqa


def get_config(config_file: Path) -> DBgenConfigParser:
    config = DBgenConfigParser(default_config=parameterized_config(DEFAULT_CONFIG))
    if not config_file.exists():
        TEMPLATE_START = '# ----------------------- TEMPLATE BEGINS HERE -----------------------'
        logger.info('Creating new DBgen config file in: %s', config_file)
        with open(config_file, 'w') as file:
            cfg = parameterized_config(DEFAULT_CONFIG)
            cfg = cfg.split(TEMPLATE_START)[-1].strip()
            file.write(cfg)
    # Read the path if it exists
    if config_file:
        config.read(config_file.absolute())
    else:
        logger.debug("No Config file provided. Using default values for DBgen variables")
    return config


config = get_config(Path(DBGEN_CONFIG))
# TODO DEFAULT_ENV and DBGEN_TMP cannot be overwritten at runtime must be stored in env var or config file
# Would be nice to make these changeable at runtime
DBGEN_TMP_STR = environ.get("DBGEN_TMP") or config.get("core", "dbgen_tmp", fallback=tempfile.gettempdir())
DBGEN_TMP = Path(DBGEN_TMP_STR)
DBGEN_TMP.mkdir(exist_ok=True, parents=True)
