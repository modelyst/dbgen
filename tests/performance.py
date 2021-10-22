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

"""A framework for testing the insertion of many entities."""
import sys
from collections import defaultdict
from time import time
from typing import Callable
from typing import Generator as GenType
from typing import Optional

from sqlalchemy import insert
from sqlalchemy.orm import registry, sessionmaker
from sqlmodel import Field

from dbgen.core.entity import BaseEntity
from tests.example.database import sql_engine

Session = sessionmaker(bind=sql_engine)
my_registry = registry()


class Car(BaseEntity, registry=my_registry, table=True):
    id: Optional[int] = Field(
        None, primary_key=True, sa_column_kwargs={"unique": True, "autoincrement": True}
    )
    make: str = Field(..., nullable=False)
    model: str = Field(..., nullable=False)


def car_maker(n_rows: int) -> GenType:
    for i in range(n_rows):
        yield Car(make="subaru", model=i)


def make_db():
    Car.metadata.create_all(sql_engine)
    yield
    Car.metadata.drop_all(sql_engine)
    yield


def main(insert_func: Callable, n_rows: int):
    delete_db = make_db()
    next(delete_db)
    start = time()
    try:
        func_runtime = insert_func(n_rows)
    finally:
        runtime = round(time() - start, 3)
        print(f"Took {insert_func.__name__} {runtime} (s) to insert {n_rows}")
        next(delete_db)
    return func_runtime


def session_add(n_rows: int):
    session = Session()

    start = time()
    cars = [car for car in car_maker(n_rows)]
    session.add_all(cars)
    session.commit()
    session.close()
    return time() - start


def session_add_no_car_gen(n_rows: int):
    session = Session()

    cars = [car for car in car_maker(n_rows)]
    start = time()
    session.add_all(cars)
    session.commit()
    session.close()
    return time() - start


def execute_many(n_rows: int):
    session = Session()
    start = time()
    statement = insert(Car).values([{"make": "subaru", "model": str(i)} for i in range(n_rows)])
    session.execute(statement)
    session.commit()
    runtime = time() - start
    return runtime


def copy_insert(n_rows: int):
    start = time()
    raw_conn = sql_engine.raw_connection()
    rows = [("subaru", str(i)) for i in range(n_rows)]
    Car._quick_load(raw_conn, rows, column_names=["make", "model"])
    return time() - start


if __name__ == "__main__":

    n_rows = int(sys.argv[1])
    output = defaultdict(list)
    max_int = 6
    vals = list(map(lambda x: int(10 ** x), range(max_int)))
    for n_rows in vals:
        print("#" * 64)
        for func in (copy_insert, execute_many, session_add, session_add_no_car_gen):
            Car.metadata.create_all(sql_engine)
            output[func.__name__].append(main(func, n_rows))
            Car.metadata.drop_all(sql_engine)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    for key, val in output.items():
        ax.plot(vals, val, label=key)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_ylabel("Insertion Time (s)")
    ax.set_xlabel("Number of rows inserted")
    plt.legend()
    plt.show()
