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

"""Parsing utilities for extracting dependencies from SQL Alchemy select statements."""
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

from sqlalchemy.orm.util import _ORMJoin
from sqlalchemy.schema import Table as SATable
from sqlalchemy.sql.elements import (
    BinaryExpression,
    BindParameter,
    ClauseList,
    ColumnClause,
    ColumnElement,
    TextClause,
)
from sqlalchemy.sql.expression import Alias, BooleanClauseList, Function, Label  # type: ignore
from sqlalchemy.sql.expression import Select as _Select
from sqlalchemy.sql.schema import Column as SAColumn

from dbgen.exceptions import QueryParsingError
from dbgen.utils.lists import flatten

# TODO Allow for arrow expressions


def expand_clause_list(clause_statement):
    output = []
    if getattr(clause_statement, "clauses", None) is not None:
        for element in getattr(clause_statement, "clauses"):
            if getattr(clause_statement, "clauses", None) is not None:
                output.extend(expand_clause_list(element))
            else:
                output.append(element)
    else:
        return [clause_statement]
    return output


def get_where_dependency(where_stmt: BooleanClauseList):
    tables = []
    columns = []
    fks = []
    for child in where_stmt.get_children():
        if isinstance(child, SATable):
            tables.append(child)
        elif isinstance(child, SAColumn) and child.foreign_keys:
            fks.append(child)
        elif isinstance(child, SAColumn):
            columns.append(expand_col(child))
        elif isinstance(child, (_ORMJoin, ColumnElement)):
            c_tables, c_columns, c_fks = get_from_dependency(child)
            tables.extend(c_tables)
            columns.extend(c_columns)
            fks.extend(c_fks)
        else:
            print(type(child))
    return columns, tables, fks


def expand_col(column: ColumnElement):
    if isinstance(column, SAColumn):
        if isinstance(column.table, Alias):
            if isinstance(column.table.original, SATable):
                return [column.table.original.columns.get(column.name)]
            else:
                raise NotImplementedError(column.table.original, type(column.table.original))
        return [column]
    elif isinstance(column, Label):
        return expand_col(column.element)
    elif isinstance(column, BinaryExpression):
        return [*expand_col(column.left), *expand_col(column.right)]
    elif isinstance(column, (BindParameter, TextClause)):
        return []
    elif getattr(column, 'get_children', None):
        return list(flatten(map(expand_col, column.get_children())))
    else:
        raise NotImplementedError(f"Unknown selected column type:\n{column}\n{type(column)}")


def get_select_dependency(select_stmt: _Select):
    selected = set(select_stmt.selected_columns)
    columns = set()
    for col in selected:
        columns.update(expand_col(col))

    if select_stmt._having_criteria is not None:  # type: ignore
        for criteria in select_stmt._having_criteria:  # type: ignore
            columns.update(expand_col(criteria))

    tables = {column.table for column in columns}
    fks = {column for column in columns if column.foreign_keys}

    return columns, tables, fks


def get_from_dependency(from_statement: Union[_ORMJoin, ColumnElement, ClauseList]):
    tables = set()
    columns = set()
    fks = set()
    for child in from_statement.get_children():
        if isinstance(child, Alias):
            tables.add(child.original)
        elif isinstance(child, SATable):
            tables.add(child)
        elif isinstance(child, SAColumn) and child.foreign_keys:
            fks.add(child)
        elif isinstance(child, SAColumn):
            columns.update(expand_col(child))
        elif isinstance(child, (_ORMJoin, ColumnElement, ClauseList)):
            c_columns, c_tables, c_fks = get_from_dependency(child)
            tables.update(c_tables)
            columns.update(c_columns)
            fks.update(c_fks)
        else:
            print(type(child))
    return columns, tables, fks


def get_statement_dependency(
    select_stmt: _Select,
) -> Tuple[Set, Set, Set]:
    """Parses a sqlalchemy select statment to get its dependencies.

    Parses the select, where, order_by, and group by clause for all properties, tables, and foreign keys
    required to run the query.

    Args:
        select_stmt (_Select): A SQL alchemy select statement

    Returns:
        Tuple[Set[str],Set[str],Set[str]]: 3 sets of the column, table, and fk depencies
    """
    # Parse the select clause
    select_cols, select_tabs, select_fks = get_select_dependency(select_stmt)
    # Parse the where clause
    if select_stmt.whereclause is not None:
        where_cols, where_tabs, where_fks = get_from_dependency(select_stmt.whereclause)
    else:
        where_cols, where_tabs, where_fks = set(), set(), set()

    from_cols, from_fks, from_tabs = set(), set(), set()
    # compile the from statements
    from_statements = select_stmt.get_final_froms()  # type: ignore
    if len(from_statements) > 1:
        print("Warning: 2 from statements detected possible crossjoin used")
    # Add the order_by clause if set
    if select_stmt._order_by_clause is not None:  # type: ignore
        from_statements.append(select_stmt._order_by_clause)  # type: ignore
    # Add the group_by clause if set
    if select_stmt._group_by_clause is not None:  # type: ignore
        from_statements.append(select_stmt._group_by_clause)  # type: ignore

    for from_statement in from_statements:
        c_from_cols, c_from_tabs, c_from_fks = get_from_dependency(from_statement)
        from_cols.update(c_from_cols)
        from_tabs.update(c_from_tabs)
        from_fks.update(c_from_fks)

    # merge dependencies
    cols = select_cols | where_cols | from_cols
    tabs = select_tabs | where_tabs | from_tabs
    fks = select_fks | where_fks | from_fks
    return cols, tabs, fks


def _get_select_keys(select_statement: _Select) -> List[str]:
    output_keys: Dict[str, Optional[str]] = {}

    for column in select_statement.selected_columns:
        col_key, marker = _parse_column(column)
        if col_key in output_keys:
            raise ValueError(
                f"Ambiguous Column Name: Both {marker} and {output_keys[col_key]} have column {col_key}.\n"
                "Please alias one of them using something like col.label('alias')"
            )
        output_keys[col_key] = marker
    return list(output_keys.keys())


def _parse_column(column: Any) -> Tuple[str, Optional[str]]:

    try:
        if isinstance(column, SAColumn):
            col_key = column.name
            marker = f"{column.table.name}.{col_key}"
            return (col_key, marker)
        elif isinstance(column, Alias):
            col_key = column.name  # type: ignore
            marker = f"{column.table.name}.{col_key}"  # type: ignore
            return (col_key, marker)
        elif isinstance(column, Label):
            col_key = column.key
            expanded_col = expand_col(column)
            marker = expanded_col
            return (col_key, marker)
        elif isinstance(column, ColumnClause):
            expanded_col = expand_col(column)
            if len(expanded_col) == 0:
                if isinstance(str, column.name):
                    column.name = cast(str, column.name)  # type: ignore
                    return (column.name, None)
                raise QueryParsingError(f"Unknown column name: {column}")
        elif isinstance(column, (Function, BinaryExpression)):
            raise QueryParsingError(
                "SQLAlchemy Functions need to be labeld due to the imprecise naming of function columns in sqlalchemy.\n"
                f"Try something like \"{getattr(column,'name','COLUMN_NAME')}(YOUR_COLUMN).label('my_column_name')\""
            )
    except QueryParsingError:
        raise
    except AttributeError as exc:
        raise QueryParsingError(
            "Error occurred during parsing that led to an internal sqlalchemy error\n" f"Column: {column}"
        ) from exc

    raise NotImplementedError(str(column), type(column))
