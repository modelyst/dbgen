"""test.py"""
from psycopg2 import connect, ProgrammingError

conn = connect(
    host="postgres", port=5432, dbname="dbgen", user="dbgen", password="dbgen"
)


def execute(statement: str):
    with conn:
        with conn.cursor() as curs:
            curs.execute(statement)
            if curs.rowcount > 0:
                try:
                    print(curs.fetchall())
                except ProgrammingError:
                    pass


execute("Select id, name as first_name from test;")
execute("create table test_2 (id serial primary key)")
