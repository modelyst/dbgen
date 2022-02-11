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

import sqlalchemy.types as types
from sqlalchemy import Column
from sqlalchemy.orm import registry
from sqlmodel import Field, select

from dbgen.core.decorators import transform
from dbgen.core.entity import Entity
from dbgen.core.etl_step import ETLStep
from dbgen.core.model import Model
from dbgen.core.node.extract import Extract
from dbgen.core.node.query import Query

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


class CustomExtractor(Extract[int]):
    n: int = 1000

    def extract(self):
        for i in range(self.n):
            yield {'out': str(i)}

    def length(self, **_):
        return self.n


@transform
def failing_func():
    raise ValueError("Failed")


@transform
def inputs_skipped():
    from dbgen.exceptions import DBgenSkipException

    raise DBgenSkipException(msg="Skip!")


def make_model():
    with Model(name='new_api', registry=my_registry) as model:
        with ETLStep('add_parent'):
            new_extract = CustomExtractor(n=1000)
            Parent.load(insert=True, label=new_extract["out"], validation='strict', myColumn={'a': 1})

        with ETLStep('add_parents_v2'):
            Parent.load(insert=True, label="parentier")

        with ETLStep('add_parents_v3'):
            Parent.load(insert=True, label="parent")

        @transform
        def concise_func(label: str) -> str:
            return f"{label}-test"

        with ETLStep('add_child'):
            query = Query(select(Parent.id, Parent.label))
            parent_id, parent_label = query.results()
            concise_pyblock = concise_func(query["label"])
            Child.load(insert=True, label=concise_pyblock.results(), parent_id=query["id"])

        with ETLStep('failing_etl_step'):
            failing_func()

        with ETLStep('skip_etl_step'):
            inputs_skipped()
    return model
