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
from pathlib import Path
from textwrap import dedent

# from pydantic.dataclasses import dataclass
from pydantic import BaseSettings, PostgresDsn, SecretStr
from pydantic.tools import parse_obj_as


# Force postgresql schemes for connection for sqlalchemy
class PostgresqlDsn(PostgresDsn):
    allowed_schemes = {"postgresql"}
    path: str


class DBgenConfiguration(BaseSettings):
    """Settings for the pg4j, especially database connections."""

    postgres_dsn: PostgresqlDsn = parse_obj_as(PostgresqlDsn, "postgresql://postgres@localhost:5432/dbgen")
    postgres_password: SecretStr = ""  # type: ignore
    postgres_schema: str = "public"
    temp_dir: Path = Path(tempfile.gettempdir())

    class Config:
        """Pydantic configuration"""

        env_file = os.environ.get("DBGEN_CONFIG", ".env")
        env_prefix = "DBGEN_"
        extra = "forbid"

    def display(self, show_passwords: bool = False):
        params = [
            "dbgen_{} = {}".format(
                key,
                val.get_secret_value() if show_passwords and "password" in key else val,
            )
            for key, val in self.dict().items()
            if key in self.__fields_set__
        ]
        params_str = "\n".join(params)
        output = f"""# DBgen Settings\n{params_str}"""
        return dedent(output)

    def __str__(self):
        return self.display()

    def __repr__(self):
        return self.display()


config = DBgenConfiguration()
