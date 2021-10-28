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

from json import dumps
from typing import Optional
from uuid import UUID

import sqlalchemy.types as types
from sqlalchemy import Column
from sqlalchemy.orm import registry
from sqlmodel import Field, select

from dbgen.core.args import Const
from dbgen.core.entity import Entity
from dbgen.core.generator import Generator
from dbgen.core.model import Model
from dbgen.core.node.extract import Extract
from dbgen.core.node.query import Query
from dbgen.core.node.transforms import PyBlock, apply_pyblock

my_registry = registry()


class Parent(Entity, registry=my_registry, table=True):
    __identifying__ = {"label"}
    label: str
    myColumn: Optional[dict] = Field(None, sa_column=Column(types.JSON()))


class Child(Entity, registry=my_registry, table=True):
    __identifying__ = {"label", "parent_id"}
    label: str
    new_col: str = "test"
    parent_id: Optional[UUID] = Field(None, foreign_key="public.parent.id")


class CustomExtractor(Extract):
    def extract(self):
        for i in range(1000):
            yield {'out': str(i)}


def failing_func():
    raise ValueError("Failed")


def inputs_skipped():
    from dbgen.exceptions import DBgenSkipException

    raise DBgenSkipException(msg="Skip!")


def make_model():
    new_extract = CustomExtractor()
    generator_1 = Generator(
        name="add_parents",
        extract=new_extract,
        loads=[Parent.load(insert=True, label=new_extract["out"], myColumn=Const(dumps({'a': 1})))],
    )
    generator_2 = Generator(
        name="add_parents_v2",
        loads=[Parent.load(insert=True, label=Const(val="parentier"))],
    )
    generator_4 = Generator(
        name="add_parents_v3",
        loads=[Parent.load(insert=True, label=Const(val="parent"))],
    )

    query = Query(select(Parent.id, Parent.label))

    @apply_pyblock(inputs=[query["label"]])
    def concise_pyblock(label: str):
        return f"{label}-test"

    child_load = Child.load(insert=True, label=concise_pyblock["out"], parent_id=query["id"])
    generator_3 = Generator(
        name="add_child",
        extract=query,
        transforms=[concise_pyblock],
        loads=[child_load],
    )
    fail_pyblock = PyBlock(
        function=failing_func,
    )
    generator_5 = Generator(name="failing_gen", transforms=[fail_pyblock])
    skip_pyblock = PyBlock(
        function=inputs_skipped,
    )
    generator_6 = Generator(name="skip_gen", transforms=[skip_pyblock])
    model = Model(
        name="test",
        generators=[
            generator_1,
            generator_2,
            generator_3,
            generator_4,
            generator_5,
            generator_6,
        ],
        registry=my_registry,
    )
    return model


def test():

    m = make_model()
    test = m._get_model_row()
    print(test.graph_json)


if __name__ == '__main__':
    test()
