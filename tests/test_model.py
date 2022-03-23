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

import pytest
from pydantic import ValidationError
from sqlalchemy import exc, select
from sqlalchemy.orm import registry

from dbgen.core.dependency import Dependency
from dbgen.core.entity import Entity
from dbgen.core.etl_step import ETLStep
from dbgen.core.model import Model
from dbgen.exceptions import ModelRunError


def test_etl_step_validation():
    model = Model(name="test_model")
    test_etl_step = ETLStep(name="test")
    model.etl_steps = [test_etl_step]
    with pytest.raises(ValidationError):
        Model(name="test", etl_steps=[test_etl_step, test_etl_step])
    with pytest.raises(ValueError):
        model.add_etl_step(test_etl_step)


def test_basic_etl_step_graph():
    a_etl_step = ETLStep(name="yields_a", additional_dependencies=Dependency(tables_yielded={"a"}))
    b_etl_step = ETLStep(
        name="yields_b",
        additional_dependencies=Dependency(tables_needed={"a"}, tables_yielded={"b"}),
    )
    c_etl_step = ETLStep(
        name="yields_c",
        additional_dependencies=Dependency(tables_needed={"a", "b"}, tables_yielded={"c"}),
    )
    d_etl_step = ETLStep(
        name="yields_d",
        additional_dependencies=Dependency(tables_needed={"c"}, tables_yielded={"d"}),
    )
    Model(name="test", etl_steps=[a_etl_step, b_etl_step, c_etl_step, d_etl_step])


@pytest.mark.database
def test_model_sync(sql_engine):
    sa_registry = registry()

    class Dummy(Entity, table=True, registry=sa_registry):
        pass

    class DummyOtherSchema(Entity, table=True, registry=sa_registry):
        __schema__ = "other_schema"

    model = Model(name="test_model", registry=sa_registry)
    model.build(sql_engine, sa_registry.metadata, build_all=True)

    for stmt in (select(Dummy.id), select(DummyOtherSchema.id)):
        with sql_engine.connect() as conn:
            with pytest.raises(exc.ProgrammingError):
                conn.execute(stmt).one_or_none()

    model.sync(sql_engine, sql_engine, True)
    with sql_engine.connect() as conn:
        result = conn.execute(select(Dummy.id)).one_or_none()
        assert result is None
        result = conn.execute(select(DummyOtherSchema.id)).one_or_none()
        assert result is None

    model.build(sql_engine, sa_registry.metadata, schemas=["other_schema"])


def test_empty_model(sql_engine):
    """Test the error produced when an empty model is run."""
    model = Model(name='test')
    with pytest.raises(ModelRunError, match='Model test has no ETLSteps'):
        model.run(sql_engine, sql_engine)
