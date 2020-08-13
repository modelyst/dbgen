"""Common utilities for interfacing with SQL Databases."""
# External Modules
from typing import List, Any, Tuple as T
from time import sleep
from io import StringIO
from psycopg2 import Error, ProgrammingError  # type: ignore
from psycopg2.extras import DictCursor, execute_batch  # type: ignore
from random import getrandbits

Connection = Any
# ##############################################################################
# # Interface with DB
# # ------------------

###########
# Shortcuts
###########


def mkInsCmd(tabName: str, names: List[str], pk: str = None) -> str:
    """
    Creates an insert command for a table named tabName with columns named names.

    Args:
        tabName (str): name of the table to insert into
        names (List[str]): list of names of columns to insert into
        pk (str, optional): name of the primary key of the table if it is not {tabName}_id. Defaults to {tabName}_id.

    Returns:
        str: Sql statement for the insertion with correct formmatting
    """
    dup = " ON CONFLICT ({}) DO NOTHING".format(pk or tabName + "_id")
    ins_names = ",".join(['"%s"' % (n) for n in names])
    fmt_args = [tabName, ins_names, ",".join(["%s"] * len(names)), dup]
    return "INSERT INTO {} ({}) VALUES ({}) {}".format(*fmt_args)


def mkUpdateCmd(tabName: str, names: List[str], keys: List[str]) -> str:
    """
    Creates an update command for a table named tabName with columns named names.

    Args:
        tabName (str): name of the table to insert into
        names (List[str]): list of names of columns to update
        keys (List[str]): where statements to update on.

    Returns:
        str: Sql statement for the update with correct formmatting
    """
    fmt_args = [tabName, addQs(names, ","), addQs(keys, " AND ")]
    return "UPDATE {} SET {} WHERE {}".format(*fmt_args)


def mkSelectCmd(tabName: str, get: List[str], where: List[str]) -> str:
    fmt_args = [",".join(get), tabName, addQs(where, " AND ")]
    return "SELECT {} FROM {} WHERE {}".format(*fmt_args)


##############################################################################


def select_dict(conn: Connection, q: str, binds: list = []) -> List[dict]:
    # print('SELECTING with: \n'+sub(q,binds))
    with conn.cursor(cursor_factory=DictCursor) as cxn:
        cxn.execute(q, vars=binds)
        return cxn.fetchall()


def sqlselect(conn: Connection, q: str, binds: list = []) -> List[tuple]:
    # print('\n\nSQLSELECT ',q)#,binds)
    with conn.cursor() as cxn:  # type: ignore
        cxn.execute(q, vars=binds)
        return cxn.fetchall()


def sqlexecute(conn: Connection, q: str, binds: list = []) -> list:
    with conn.cursor() as cxn:  # type: ignore
        while True:
            try:
                cxn.execute(q, vars=binds)
                try:
                    out = cxn.fetchall()
                    return out
                except ProgrammingError as e:
                    if "no results to fetch" in e.args:
                        return []
                    else:
                        raise Error(e)
            except Error as e:
                if e.args[0] in [1205, 1213]:  # deadlock error codes
                    print("SLEEPING")
                    sleep(10)
                elif e.pgcode in ["42701"]:
                    raise e
                else:
                    raise Error(e)


def sqlexecutemany(conn: Connection, q: str, binds: List[list]) -> None:
    with conn.cursor() as cxn:  # type: ignore
        while True:
            try:
                execute_batch(cur=cxn, sql=q, argslist=binds)
                # return cxn.fetchall()
                break
            except Error as e:
                if e.args[0] in [1205, 1213]:  # deadlock error codes
                    print("SLEEPING")
                    sleep(10)
                else:
                    raise Error(e)


def addQs(xs: list, delim: str) -> str:
    """
    Ex: ['a','b','c'] + ',' ==> 'a = %s, b = %s, c = %s'
    """
    return delim.join(["{0} = %s".format(x) for x in xs])


def batched_cursor(cursor: Any, arraysize: int = 1000) -> Any:
    """An iterator that uses fetchmany to keep memory usage down"""
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


def fast_load(
    conn: Connection,
    rows: T[List[Any], ...],
    table_name: str,
    col_names: List[str],
    obj_pk_name: str,
) -> None:

    # write rows to string io object to allow copy_from to be used
    io_obj = StringIO()
    for row in rows:
        io_obj.write("\t".join(map(str, row)) + "\n")

    io_obj.seek(0)
    # Temporary table to copy data into
    # Set name to be hash of input rows to ensure uniqueness for parallelization
    temp_table_name = table_name + "_temp_load_table_" + str(getrandbits(64))

    # Need to create a temp table to copy data into
    # Add an auto_inc column so that data can be ordered by its insert location
    create_temp_table = """
        DROP TABLE IF EXISTS {temp_table_name};
        CREATE TEMPORARY TABLE {temp_table_name} AS
        TABLE {obj}
        WITH NO DATA;
        ALTER TABLE {temp_table_name}
        ADD COLUMN auto_inc SERIAL NOT NULL;
        """.format(
        obj=table_name, temp_table_name=temp_table_name
    )
    from dbgen.templates import jinja_env

    insert_template = jinja_env.get_template("insert.sql.jinja")
    template_args = dict(
        obj=table_name,
        obj_pk_name=obj_pk_name,
        temp_table_name=temp_table_name,
        all_column_names=col_names,
        first=False,
        update=True,
    )
    insert_statement = insert_template.render(**template_args)
    with conn.cursor() as curs:
        curs.execute(create_temp_table)
        curs.copy_from(io_obj, temp_table_name, null="None", columns=col_names)
        curs.execute(insert_statement)

