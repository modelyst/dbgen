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

"""simple_model.py"""

from typing import Optional
from uuid import UUID

from pydantic import Field
from sqlmodel import Session, select

from dbgen.core.args import Const
from dbgen.core.entity import Entity
from dbgen.core.generator import Generator
from dbgen.core.model import Model
from dbgen.core.node.extract import Extract
from dbgen.core.node.query import Query
from dbgen.core.node.transforms import PyBlock, apply_pyblock
from tests.example.database import sql_engine


class BaseTable(Entity):
    label: str
    type: str
    __identifying__ = {"label", "type"}


class Parent(BaseTable, table=True):
    non_id_col: int = Field(42, sa_column_kwargs={"server_default": "42"})


class Child(BaseTable, table=True):
    parent_id: Optional[UUID] = Parent.foreign_key()
    __identifying__ = {"parent_id"}


class ProcessData(Entity, table=True):
    file_name: str
    __identifying__ = {"file_name"}


class IntExtractor(Extract):
    max_int: int = 10000

    def extract(self, *args, **kwargs):
        for i in range(self.max_int):
            yield {self.hash: {"out": i}}

    def get_row_count(self, connection=None, *args) -> Optional[int]:
        return self.max_int


def make_model() -> Model:
    model = Model(name="tests")

    # Load First Parent
    parent_load = Parent.load(insert=True, label=Const("Ken"), type=Const("Engineer"))
    child_load = Child.load(insert=True, label=Const("Test"), type=Const("Test"), parent_id=parent_load)
    gen_1 = Generator(name="load_first_parent", loads=[child_load])
    model.add_gen(gen_1)

    # Load First Parent
    query = Query(select(Parent.id))
    child_load = Child.load(insert=True, label=Const("Brian"), type=Const("surfer"), parent_id=query["id"])
    gen_2 = Generator(name="load_first_child", extract=query, loads=[child_load])
    model.add_gen(gen_2)
    # Load Transform Parent
    query = Query(select(Parent.id, Parent.label))

    @apply_pyblock(inputs=[query["label"]])
    def simple_pyblock(label: str):
        label += 'test'
        return len(label)

    add_one = lambda x: x + 1
    add_one_pb = PyBlock(function=add_one, inputs=[simple_pyblock["out"]])

    parent_update = Parent.load(parent=query["id"])
    gen_3 = Generator(
        name="update_parent",
        extract=query,
        transforms=[simple_pyblock, add_one_pb],
        loads=[parent_update],
    )
    model.add_gen(gen_3)

    @apply_pyblock(inputs=[query["label"]])
    def simple_pyblock(label: str):
        from time import sleep

        sleep(1)
        return len(label)

    add_one = lambda x: x + 1
    add_one_pb = PyBlock(function=add_one, inputs=[simple_pyblock["out"]])

    parent_update = Parent.load(parent=query["id"])
    gen_4 = Generator(
        name="slow_gen",
        extract=query,
        transforms=[simple_pyblock, add_one_pb],
        loads=[parent_update],
    )
    model.add_gen(gen_4)

    # Load into process data
    # file_extractor = FileExtractor(directory="./test_files")

    # int_extractor = IntExtractor(max_int=30)
    # get_file_name = lambda x: f"plate_{x}.txt"
    # pb = PyBlock(function=get_file_name, inputs=[int_extractor["out"]])
    # pd_insert = ProcessData.load(insert=True, file_name=pb["out"])
    # gen_5 = Generator(
    #     name="load files new",
    #     extract=int_extractor,
    #     transforms=[pb],
    #     loads=[pd_insert],
    # )
    # # model.add_gen(gen_5)

    return model


def get_parent_by_name(name: str):
    session = Session(sql_engine)
    parent = session.exec(select(Parent).where(Parent.label == name)).one()
    return parent


def main():
    model = make_model()
    model.run(
        sql_engine,
    )


# class GeneratorOperator(Operator):
#     def __init__(self, run_id, ordering):
#         self.run_id = run_id
#         self.ordering = ordering

#     def execute(self):
#         cmd = "dbgen rungen {self.run_id} {self.ordering} "
#         subprocess.check_call(cmd)


# with Dag(name={model.name}) as dag:
#     for i in len(model.generators):
#         gen_1 = GeneratorOperator(1, 1)
#         gen_2 = GeneratorOperator(1, 2)
#         gen_3 = GeneratorOperator(1, 3)
#     gen_1 >> gen_2
#     gen_2 >> gen_3
if __name__ == "__main__":
    main()
