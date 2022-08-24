#   Copyright 2022 Modelyst LLC
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

import pytest
from sqlalchemy.future import Engine
from sqlalchemy.orm import registry
from sqlmodel import Session, select

from dbgen.core.args import Constant
from dbgen.core.entity import Entity
from dbgen.core.etl_step import ETLStep
from dbgen.core.model import Model
from dbgen.core.node.query import Query
from dbgen.utils.typing import IDType

test_registry = registry()


class Father(Entity, table=True, registry=test_registry):
    __identifying__ = {'first_name', 'last_name'}
    first_name: str
    last_name: str
    age: int


class Son(Entity, table=True, registry=test_registry):
    __identifying__ = {'first_name', 'last_name'}
    first_name: str
    last_name: str
    age: int
    father_id: IDType = Father.foreign_key()


@pytest.fixture
def simple_model():
    with Model(name='test', registry=test_registry) as model:
        with ETLStep('add_parent'):
            Father.load(
                insert=True, age=Constant(42), first_name=Constant('Homer'), last_name=Constant('Simpson')
            )
        with ETLStep('add_child'):
            father_id = Query(
                select(Father.id).where(Father.first_name == 'Homer').where(Father.last_name == 'Simpson')
            ).results()
            Son.load(
                insert=True,
                age=Constant(12),
                father_id=father_id,
                first_name=Constant('Bart'),
                last_name=Constant('Simpson'),
            )

    return model


def test_model(simple_model: Model):
    assert len(simple_model.etl_steps) == 2


@pytest.mark.parametrize('run_async', (False, True), ids=['sync', 'async'])
def test_full_model_run(simple_model: Model, sql_engine: Engine, run_async: bool):
    run = simple_model.run(sql_engine, sql_engine, build=True, run_async=run_async)
    assert run.status == 'completed'
    with Session(sql_engine) as session:
        parent, child = session.exec(select(Father, Son).join(Son).where(Son.first_name == 'Bart')).one()
        assert parent.first_name == 'Homer'
        assert parent.last_name == 'Simpson'
        assert child.first_name == 'Bart'
        assert child.last_name == 'Simpson'
