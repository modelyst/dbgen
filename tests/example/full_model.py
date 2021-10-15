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

from typing import Optional
from uuid import UUID

from sqlalchemy.orm import registry
from sqlmodel import Field, select

from dbgen.core.args import Const
from dbgen.core.entity import EntityId
from dbgen.core.extract import Extract
from dbgen.core.generator import Generator
from dbgen.core.model import Model
from dbgen.core.query import Query
from dbgen.core.transforms import PyBlock, apply_pyblock

my_registry = registry()


class Parent(EntityId, registry=my_registry, table=True):
    __identifying__ = {"label"}
    label: str


class Child(EntityId, registry=my_registry, table=True):
    __identifying__ = {"label", "parent_id"}
    label: str
    new_col: str = "test"
    parent_id: Optional[UUID] = Field(None, foreign_key="public.parent.id")


class NewExtract(Extract):
    def extract(self, *args, **kwargs):
        for i in range(1000):
            yield {self.hash: {"out": i}}

    def get_row_count(self, **kwargs):
        return 1000


def failing_func():

    raise ValueError("Failed")


def inputs_skipped():
    from dbgen.exceptions import DBgenSkipException

    raise DBgenSkipException("Skip!")


def make_model():
    new_extract = NewExtract()
    generator_1 = Generator(
        name="add_parents",
        extract=new_extract,
        loads=[Parent.load(insert=True, label=new_extract["out"])],
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
        from time import sleep

        sleep(0.05)
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
    generator_5 = Generator(name="failing gen", transforms=[fail_pyblock])
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
