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
from typing import Optional, Union, cast, overload

from pydantic import Field
from pydantic.networks import PostgresDsn
from pydantic.tools import parse_obj_as
from pydantic.types import SecretStr
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection as SAConnection
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlmodel.sql.expression import Select

from dbgen.core.base import Base
from dbgen.core.dependency import Dependency
from dbgen.core.extract import Extract, extractor_type
from dbgen.core.statement_parsing import _get_select_keys, get_statement_dependency

log = logging.getLogger(__name__)


SCHEMA_DEFAULT = "public"


class Connection(Base):
    scheme: str = "postgresql"
    user: str = "postgres"
    password: Optional[SecretStr] = None
    host: str = "localhost"
    port: int = 5432
    database: str = "dbgen"
    schema_: str = Field(SCHEMA_DEFAULT, alias="schema")
    _hashexclude_ = {"password"}

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.url()

    @classmethod
    def from_uri(cls, uri: Union[PostgresDsn, str], schema: str = SCHEMA_DEFAULT) -> "Connection":
        if isinstance(uri, str):
            uri = parse_obj_as(PostgresDsn, uri)
            uri = cast(PostgresDsn, uri)
        assert uri.path, f"uri is missing database {uri}"
        return cls(
            host=uri.host or "localhost",
            user=uri.user,
            password=uri.password,
            port=uri.port or 5432,
            database=uri.path.lstrip("/"),
            schema_=schema,
        )

    def url(self, mask_password: bool = True):
        if mask_password:
            password = "******"
        elif self.password:
            password = self.password.get_secret_value()
        else:
            password = ""
        return f"{self.scheme}://{self.user}:{password}@{self.host}:{self.port}/{self.database}"

    def get_engine(self):
        return create_engine(
            url=self.url(),
            connect_args={"options": f"-csearch_path={self.schema_}"},
        )

    def test(self):
        engine = self.get_engine()
        try:
            with engine.connect() as conn:
                conn.execute("select 1")
        except OperationalError as exc:
            log.error(f"Trouble connecting to database at {self}.\n{exc}")
            return False
        return True


class BaseQuery(Extract):
    query: str
    dependency: Dependency = Field(default_factory=Dependency)

    def _get_dependency(self) -> Dependency:
        return self.dependency

    @classmethod
    def from_select_statement(
        cls, select_statement: Select, connection: Connection = None, **kwargs
    ) -> "BaseQuery":
        columns, tables, fks = get_statement_dependency(select_statement)
        outputs = _get_select_keys(select_statement)
        get_table_name = lambda x: f"{x.schema}.{x.name}" if getattr(x, "schema", "") else x.name
        dependency = Dependency(
            tables_needed=[get_table_name(x) for x in tables],
            columns_needed=[f"{get_table_name(x.table)}.{x.name}" for x in columns.union(fks)],
        )
        return cls(
            inputs=[],
            outputs=outputs,
            query=str(select_statement),
            dependency=dependency,
        )

    def extract(
        self,
        *,
        connection: SAConnection = None,
        yield_per: Optional[int] = None,
        **kwargs,
    ) -> extractor_type:
        assert connection
        if yield_per:
            result = connection.execute(text(self.query))
            for row in result.yield_per(yield_per).mappings():
                yield {self.hash: row}
        else:
            result = connection.execute(text(self.query)).mappings().all()
            for row in result:
                yield {self.hash: row}

    def get_row_count(self, *, connection: SAConnection = None) -> int:
        assert connection
        count_statement = f"select count(1) from ({self.query}) as X;"
        rows = connection.execute(text(count_statement)).scalar()
        return rows


class ExternalQuery(BaseQuery):
    connection: Connection

    @classmethod
    def from_select_statement(
        cls, select_statement: Select, connection: Connection = None, **kwargs
    ) -> "ExternalQuery":
        selected_keys = _get_select_keys(select_statement)
        return cls(
            query=str(select_statement),
            outputs=selected_keys,
            connection=connection,
        )

    def extract(
        self, *, connection: SAConnection = None, yield_per: Optional[int] = None, **kwargs
    ) -> extractor_type:
        engine = self.connection.get_engine()
        with Session(engine) as session:
            if yield_per:
                result = session.execute(text(self.query))
                yield from result.yield_per(yield_per).mappings()
            else:
                result = session.execute(text(self.query)).mappings().all()
                yield from result


@overload
def Query(select_statement: Select, connection: Connection) -> ExternalQuery:
    ...


@overload
def Query(select_statement: Select, connection: None = None) -> BaseQuery:
    ...


def Query(
    select_statement: Select, connection: Optional[Connection] = None
) -> Union[BaseQuery, ExternalQuery]:
    cls = BaseQuery if connection is None else ExternalQuery
    return cls.from_select_statement(select_statement, connection=connection)
