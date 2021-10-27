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

from typing import TYPE_CHECKING, Any, Dict
from typing import Generator as GenType
from typing import Optional, Union, overload

from pydantic import Field
from sqlalchemy import text
from sqlmodel.sql.expression import Select

from dbgen.core.dependency import Dependency
from dbgen.core.node.extract import Extract
from dbgen.core.statement_parsing import _get_select_keys, get_statement_dependency
from dbgen.utils.sql import Connection

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection as SAConnection
    from sqlmodel import Session


SCHEMA_DEFAULT = "public"


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

    def length(self, *, connection: 'SAConnection' = None) -> int:
        assert connection
        count_statement = f"select count(1) from ({self.query}) as X;"
        rows: int = connection.execute(text(count_statement)).scalar()  # type: ignore
        return rows

    def setup(
        self,
        connection: Union['SAConnection', 'Session'] = None,
        yield_per: Optional[int] = None,
        **kwargs,
    ) -> GenType[Dict[str, Any], None, None]:
        assert connection, f"Need to pass in connection when setting the extractor"
        if yield_per:
            result = connection.execute(text(self.query))
            return result.yield_per(yield_per).mappings()
        else:
            result = connection.execute(text(self.query))
            return result.mappings()


class ExternalQuery(BaseQuery):
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

    def set_extractor(
        self,
        *,
        connection: Union['SAConnection', 'Session'] = None,
        yield_per: Optional[int] = None,
        **kwargs,
    ) -> None:
        engine = self.connection.get_engine()
        with engine.connect() as conn:
            if yield_per:
                result = conn.execute(text(self.query))
                self._extractor = result.yield_per(yield_per).mappings()
            else:
                result = conn.execute(text(self.query))
                self._extractor = result.mappings()


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
