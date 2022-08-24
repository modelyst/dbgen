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

from typing import TYPE_CHECKING, Any, Dict
from typing import Generator as GenType
from typing import Optional, Type, TypeVar, Union, overload

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlmodel.sql.expression import Select, SelectOfScalar

from dbgen.core.dependency import Dependency
from dbgen.core.node.extract import Extract
from dbgen.core.statement_parsing import _get_select_keys, get_statement_dependency
from dbgen.utils.sql import Connection

if TYPE_CHECKING:
    from psycopg import AsyncConnection  # pragma: no cover
    from sqlalchemy.engine import Connection as SAConnection  # pragma: no cover


SCHEMA_DEFAULT = "public"

T = TypeVar('T')

postgresql_dialect = postgresql.dialect()  # type: ignore


class BaseQuery(Extract[T]):
    query: str
    params: Dict[str, Any] = Field(default_factory=dict)
    dependency: Dependency = Field(default_factory=Dependency)
    _connection: 'SAConnection'
    _yield_per: Optional[int] = None

    def _get_dependency(self) -> Dependency:
        return self.dependency

    @classmethod
    def from_select_statement(
        cls, select_statement: Select[T], connection: Connection = None, **kwargs
    ) -> "BaseQuery[T]":
        columns, tables, fks = get_statement_dependency(select_statement)
        outputs = _get_select_keys(select_statement)
        get_table_name = lambda x: f"{x.schema}.{x.name}" if getattr(x, "schema", "") else x.name
        dependency = Dependency(
            tables_needed=[get_table_name(x) for x in tables],
            columns_needed=[f"{get_table_name(x.table)}.{x.name}" for x in columns.union(fks)],
        )
        compiled_statement = select_statement.compile()
        return cls(
            inputs=[],
            outputs=outputs,
            query=str(compiled_statement),
            params=compiled_statement.params,
            dependency=dependency,
        )

    def render_query(self) -> str:
        """Stringifies the query with the bound parameters."""
        compiled_query = (
            text(self.query).bindparams(**self.params).compile(compile_kwargs={'literal_binds': True})
        )
        return str(compiled_query)

    def set_connection(self, connection: 'SAConnection', yield_per: Optional[int] = None):
        self._connection = connection
        self._yield_per = yield_per

    def length(self) -> int:
        rows: int = self._connection.execute(text(self.count_statement)).scalar()  # type: ignore
        return rows

    async def _async_length(self, *, connection: 'AsyncConnection' = None, **_) -> Optional[int]:
        assert connection
        result = await connection.execute(
            self.count_statement,
            self.params,
        )
        out = await result.fetchone()
        (count,) = out if out else (None,)
        return count

    @property
    def compiled_query(self):
        return str(text(self.query).compile(dialect=postgresql_dialect))

    @property
    def count_statement(self):
        return str(
            text(f'select count(1) from ({self.render_query()}) as X').compile(dialect=postgresql_dialect)
        )

    def extract(
        self,
    ) -> GenType[T, None, None]:
        if self._yield_per:
            result = self._connection.execution_options(stream_results=True).execute(
                text(self.query).bindparams(**self.params)
            )
            while chunk := result.fetchmany(self._yield_per):
                for row in chunk:
                    yield dict(row)  # type: ignore
        else:
            result = self._connection.execute(text(self.query).bindparams(**self.params))
            yield from result.mappings()  # type: ignore


class ExternalQuery(BaseQuery[T]):
    connection: Connection

    @classmethod
    def from_select_statement(
        cls, select_statement: Select, connection: 'Connection' = None, **kwargs
    ) -> "ExternalQuery":
        selected_keys = _get_select_keys(select_statement)
        return cls(
            query=str(select_statement),
            outputs=selected_keys,
            connection=connection,
        )

    def setup(self):
        engine = self.connection.get_engine()
        self._connection = engine.connect()

    def teardown(self):
        self._connection.close()


@overload
def Query(select_statement: SelectOfScalar[T], connection: Connection) -> ExternalQuery[T]:
    ...


@overload
def Query(select_statement: SelectOfScalar[T], connection: None = None) -> BaseQuery[T]:
    ...


@overload
def Query(select_statement: Select[T], connection: Connection) -> ExternalQuery[T]:
    ...


@overload
def Query(select_statement: Select[T], connection: None = None) -> BaseQuery[T]:
    ...


def Query(select_statement, connection: Optional[Connection] = None) -> Union[BaseQuery, ExternalQuery]:
    cls: Union[Type[BaseQuery], Type[ExternalQuery]] = BaseQuery if connection is None else ExternalQuery
    return cls.from_select_statement(select_statement, connection=connection)
