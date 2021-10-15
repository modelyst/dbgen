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
from dbgen.core.entity import EntityId
from dbgen.core.generator import Generator
from dbgen.core.model import Model


def test_generator_validation():
    model = Model(name="test_model")
    test_gen = Generator(name="test")
    model.generators = [test_gen]
    with pytest.raises(ValidationError):
        Model(name="test", generators=[test_gen, test_gen])
    with pytest.raises(ValueError):
        model.add_gen(test_gen)


def test_basic_generator_graph():
    a_gen = Generator(name="yields a", additional_dependencies=Dependency(tables_yielded={"a"}))
    b_gen = Generator(
        name="yields b",
        additional_dependencies=Dependency(tables_needed={"a"}, tables_yielded={"b"}),
    )
    c_gen = Generator(
        name="yields c",
        additional_dependencies=Dependency(tables_needed={"a", "b"}, tables_yielded={"c"}),
    )
    d_gen = Generator(
        name="yields d",
        additional_dependencies=Dependency(tables_needed={"c"}, tables_yielded={"d"}),
    )
    Model(name="test", generators=[a_gen, b_gen, c_gen, d_gen])


def test_model_sync(sql_engine, debug_logger):
    sa_registry = registry()

    class Dummy(EntityId, table=True, registry=sa_registry):
        pass

    class DummyOtherSchema(EntityId, table=True, registry=sa_registry):
        __schema__ = "other_schema"

    model = Model(name="test_model", registry=sa_registry)
    model.nuke(sql_engine, sa_registry.metadata, nuke_all=True)

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

    model.nuke(sql_engine, sa_registry.metadata, schemas=["other_schema"])


if __name__ == "__main__":
    test_basic_generator_graph()
