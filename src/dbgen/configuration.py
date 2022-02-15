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

"""For parsing in config YAML."""
import os
import tempfile
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Optional, Tuple

from pydantic import BaseSettings, PostgresDsn, SecretStr, validator
from pydantic.tools import parse_obj_as

from dbgen.utils.log import setup_logger
from dbgen.utils.sql import Connection

if TYPE_CHECKING:
    from sqlalchemy.future import Engine  # pragma: no cover


# Force postgresql schemes for connection for sqlalchemy
class PostgresqlDsn(PostgresDsn):
    allowed_schemes = {"postgresql"}
    path: str


class ValidationEnum(str, Enum):
    COERCE = 'coerce'
    STRICT = 'strict'
    OFF = 'off'


class DBgenConfiguration(BaseSettings):
    """Settings for the pg4j, especially database connections."""

    main_dsn: PostgresqlDsn = parse_obj_as(PostgresqlDsn, "postgresql://postgres@localhost:5432/dbgen")
    main_schema: str = "public"
    main_password: SecretStr = ""  # type: ignore
    meta_dsn: Optional[PostgresqlDsn]
    meta_schema: str = "dbgen_log"
    meta_password: Optional[SecretStr] = None
    model_str: str = 'model.main:make_model'
    batch_size: int = 100
    temp_dir: Path = Path(tempfile.gettempdir())
    validation: ValidationEnum = ValidationEnum.COERCE
    pdb: bool = False

    class Config:
        """Pydantic configuration"""

        env_file = os.environ.get("DBGEN_CONFIG", ".env")
        env_prefix = "DBGEN_"
        extra = "forbid"

    @validator('temp_dir')
    def validate_temp_dir(cls, temp_dir: Path) -> Path:
        if not temp_dir.exists():
            temp_dir.mkdir(exist_ok=True, parents=True)
        if temp_dir.is_dir():
            return temp_dir
        raise ValueError(f"temp_dir value {temp_dir} is not a directory")

    @validator('batch_size')
    def validate_batch_size(cls, batch_size) -> Path:
        if batch_size > 1:
            return batch_size
        raise ValueError(f'batch_size must be a postive integer: {batch_size}')

    def display(self, show_defaults: bool = False, show_passwords: bool = False):
        params = []
        for key, val in self.dict().items():
            if val is not None:
                str_val = f'{val.get_secret_value()}' if show_passwords and "password" in key else val
                if show_defaults or key in self.__fields_set__:
                    params.append(f"dbgen_{key} = {str_val}")
                else:
                    params.append(f"# dbgen_{key} = {str_val}")

        params_str = "\n".join(params)
        output = f"""# DBgen Settings\n{params_str}"""
        return dedent(output)

    def __str__(self):
        return self.display()

    def __repr__(self):
        return self.display()


config = DBgenConfiguration()
root_logger, stdout_handler = setup_logger()


def update_config(config_file: 'Path') -> DBgenConfiguration:
    global config
    input_config = DBgenConfiguration(_env_file=config_file)
    if input_config.meta_schema != config.meta_schema:
        root_logger.warning(
            f"WARNING: Input config file {str(config_file)!r} has a meta_schema value set that is different than the current value at {DBgenConfiguration.Config.env_file!r}."
            " This setting will be ignored as setting the meta_schema in an CLI input config file is not fully implemented."
            " To set this variable, please use the DBGEN_CONFIG environmental variable rather than the -c/--config CLI argument."
            f" For example, set `DBGEN_CONFIG={str(config_file)!r}` before running dbgen."
        )
    new_config_kwargs = {**config.dict(), **input_config.dict()}
    config = DBgenConfiguration.parse_obj(new_config_kwargs)
    return config


def get_connections() -> Tuple['Connection', 'Connection']:
    main_conn = Connection.from_uri(config.main_dsn, config.main_schema, password=config.main_password)
    meta_dsn = config.meta_dsn or config.main_dsn
    meta_password = config.meta_password or config.main_password
    meta_conn = Connection.from_uri(meta_dsn, config.meta_schema, meta_password)
    return (main_conn, meta_conn)


def get_engines() -> Tuple['Engine', 'Engine']:
    (main_conn, meta_conn) = get_connections()
    return (main_conn.get_engine(), meta_conn.get_engine())
