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

"""Methods related to fast copying to a postgresql database."""
import asyncio
import re
from logging import getLogger
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Optional, Sequence, Tuple
from uuid import UUID

from psycopg.errors import UndefinedColumn

from dbgen.exceptions import DatabaseError

if TYPE_CHECKING:
    from psycopg import AsyncConnection, Connection

    from dbgen.core.node.load import LoadEntity

# Error messages
MISSING_COLUMN_ERROR = "An ETLStep is trying to load into the entity {name!r}, however {column} does not exist. This commonly occurs when a column is added to an entity without being effectively synced with the database. An easy fix is to rerun the model with --build, if this is not possible you can manually add the column. If this column "
MISSING_ID_ERROR = "An ETLStep is trying to load into the entity {name!r}, however {column} does not exist. This can occur when the entities in the model comes out of sync with the database. Since {column} is an identifying column of {name!r} the id hashes for the rows in the table will change when adding this column therefore you must rebuild the database with --build."

# SQL Statements for the get_statements function
CREATE_TABLE_STATEMENT = """
CREATE TEMPORARY TABLE {temp_table_name} AS
TABLE {full_table_name}
WITH NO DATA;
ALTER TABLE {temp_table_name}
ADD COLUMN auto_inc SERIAL NOT NULL;
"""

UPDATE_STATEMENT = """
UPDATE
    {full_table_name}
SET
    {column_str}
FROM
    {temp_table_name}
WHERE
    {full_table_name}.{table_primary_key_name} = {temp_table_name}.{table_primary_key_name};
"""

INSERT_STATEMENT = """
INSERT INTO {full_table_name}
({all_columns_str})
SELECT
{all_columns_str}
FROM (
  SELECT
    {all_columns_str},
    ROW_NUMBER() OVER (PARTITION BY {table_primary_key_name}
               ORDER BY auto_inc {order}) AS row_number
  FROM
    {temp_table_name}) AS X
WHERE
  row_number = 1 ON CONFLICT {conflict_key}
  DO
  UPDATE
  SET
  {update_column_statement}
  RETURNING
    {table_primary_key_name}
"""


def escape_str(column: str):
    return f"\"{column}\""


def _get_types(load_entity: 'LoadEntity', columns):
    oids = list(map(lambda x: load_entity.attributes[x], sorted(columns)))
    return oids


def load_data(
    data: Mapping[UUID, Sequence[Any]],
    connection: 'Connection',
    load_entity: 'LoadEntity',
    columns: Iterable[str],
    insert: bool,
    temp_table_suffix: str = '',
    etl_step_id: Optional[UUID] = None,
) -> int:
    # Setup the logger
    logger = getLogger(f'dbgen.load.{load_entity.name}')
    # Get the SQL Statements for this load
    create_statement, drop_statement, copy_statement, load_statement = get_statements(
        load_entity.name,
        load_entity.full_name,
        load_entity.primary_key_name,
        insert,
        columns,
        temp_table_suffix=temp_table_suffix,
        etl_step_id=etl_step_id,
    )
    # Drop the Create the Temporary table
    logger.debug('creating temp table')
    with connection.cursor() as cur:
        cur.execute(drop_statement)
        cur.execute(create_statement)
    connection.commit()

    with connection.cursor() as cur:
        logger.debug("load into temporary table")
        try:
            with cur.copy(copy_statement) as copy:
                oids = _get_types(load_entity, [load_entity.primary_key_name] + list(columns))
                copy.set_types(oids)
                for pk_curr, row_curr in data.items():
                    copy.write_row((pk_curr, *row_curr))
        except UndefinedColumn as exc:
            # Try to match the column name to give helpful error messages
            match = re.match('column \"(\\w+)\"', str(exc))
            if match:
                (column,) = match.groups()
                column_str = f'the column {column!r}'
            else:
                column_str = 'a column'

            if column in load_entity.identifiers:
                raise DatabaseError(MISSING_ID_ERROR.format(name=load_entity.name, column=column_str))
            raise DatabaseError(
                MISSING_COLUMN_ERROR.format(name=load_entity.name, column=column_str)
            ) from exc
        # Try to insert everything from the temp table into the real table
        # If a foreign_key violation is hit, we delete those rows in the
        # temp table and move on
        logger.debug("transfer from temp table to main table")
        cur.execute(load_statement)
        logger.debug("Dropping temp table...")
        cur.execute(drop_statement)
    connection.commit()
    logger.debug("loading finished")
    return len(data)


async def async_load_data(
    data: Mapping[UUID, Sequence[Any]],
    connection: 'AsyncConnection',
    load_entity: 'LoadEntity',
    columns: Iterable[str],
    insert: bool,
    temp_table_suffix: str = '',
    etl_step_id: Optional[UUID] = None,
) -> int:
    # Setup the logger
    logger = getLogger(f'dbgen.async_load.{load_entity.name}')
    task = asyncio.current_task()
    # Get the SQL Statements for this load
    create_statement, drop_statement, copy_statement, load_statement = get_statements(
        load_entity.name,
        load_entity.full_name,
        load_entity.primary_key_name,
        insert,
        columns,
        temp_table_suffix=f'{temp_table_suffix}_{hash(task)}',
        etl_step_id=etl_step_id,
    )
    # Drop the Create the Temporary table
    logger.debug('creating temp table')
    async with connection.cursor() as cur:
        await cur.execute(drop_statement)
        logger.debug(create_statement)
        await cur.execute(create_statement)
    await connection.commit()

    async with connection.cursor() as cur:
        logger.debug("load into temporary table")
        try:
            async with cur.copy(copy_statement) as copy:
                oids = _get_types(load_entity, [load_entity.primary_key_name] + list(columns))
                copy.set_types(oids)
                for pk_curr, row_curr in data.items():
                    await copy.write_row((pk_curr, *row_curr))
        except UndefinedColumn as exc:
            # Try to match the column name to give helpful error messages
            match = re.match('column \"(\\w+)\"', str(exc))
            if match:
                (column,) = match.groups()
                column_str = f'the column {column!r}'
            else:
                column_str = 'a column'

            if column in load_entity.identifiers:
                raise DatabaseError(MISSING_ID_ERROR.format(name=load_entity.name, column=column_str))
            raise DatabaseError(
                MISSING_COLUMN_ERROR.format(name=load_entity.name, column=column_str)
            ) from exc
        # Try to insert everything from the temp table into the real table
        # If a foreign_key violation is hit, we delete those rows in the
        # temp table and move on
        logger.debug("transfer from temp table to main table")
        await cur.execute(load_statement)
        logger.debug("Dropping temp table...")
        await cur.execute(drop_statement)
    await connection.commit()
    logger.debug("loading finished")
    return len(data)


def get_statements(
    table_name: str,
    full_table_name: str,
    table_primary_key_name: str,
    insert: bool,
    columns: Iterable[str],
    etl_step_id: Optional[UUID] = None,
    temp_table_suffix: str = '',
    partition_attribute: Optional[str] = None,
) -> Tuple[str, str, str, str]:
    """
    Generate the SQL statements relevant for bulk loading data into postgresql.sql

    Args:
        table_name (str): The table name to load into postgresql.sql
        schema (str): The schema of the table to load into postgresql.sql
        load_entity_hash (str): A unique hash for this loads
        insert (bool): Whether or not the statement should be update or inserted
        etl_step_id (UUID): The ETLStep UUID that is loading this data.sql

    Returns:
        Tuple[str, str, str]: The create_table, drop_table, and load statements
    """
    # create
    sanitized_name = full_table_name.replace(".", "_").replace('"', '')
    temp_table_name = f'{sanitized_name}_temp_load_table'
    if temp_table_suffix:
        temp_table_name += f'_{temp_table_suffix}'
    temp_table_name = escape_str(temp_table_name)
    all_columns = [escape_str(table_primary_key_name)] + list(sorted(map(escape_str, columns)))

    create_statement = CREATE_TABLE_STATEMENT.format(
        temp_table_name=temp_table_name,
        table_name=table_name,
        full_table_name=full_table_name,
        etl_step_id=etl_step_id,
    )
    drop_statement = f"DROP TABLE IF EXISTS {temp_table_name}"
    # The Copy statement does not need to insert the ETLStep ID
    copy_columns_str = ', '.join(all_columns)
    copy_statement = f'COPY  {temp_table_name} ({copy_columns_str}) FROM STDIN'
    # If an etl_step_id is provided add it as a default column on temp table so each row
    # modified by these statements track their id
    if etl_step_id is not None:
        create_statement += (
            f"ALTER TABLE {temp_table_name} ALTER COLUMN \"etl_step_id\" SET DEFAULT '{etl_step_id}'"
        )
        all_columns.append(escape_str('etl_step_id'))

    column_statement = [f'{column} = {temp_table_name}.{column}' for column in all_columns]
    first = False
    full_kwargs = dict(
        temp_table_name=temp_table_name,
        table_primary_key_name=escape_str(table_primary_key_name),
        full_table_name=full_table_name,
        all_columns_str=', '.join(all_columns),
        column_str=', '.join(column_statement),
        order='ASC' if first else 'DESC',
    )

    if insert:
        if partition_attribute:
            full_kwargs['conflict_key'] = f'("{table_primary_key_name}","{partition_attribute}")'
        else:
            full_kwargs['conflict_key'] = f'({escape_str(table_primary_key_name)})'
        update_column_statement = ', '.join([f'{column} = excluded.{column}' for column in all_columns])
        full_kwargs['update_column_statement'] = update_column_statement
        load_statement = INSERT_STATEMENT.format(**full_kwargs)
    else:
        load_statement = UPDATE_STATEMENT.format(**full_kwargs)

    return create_statement, drop_statement, copy_statement, load_statement
