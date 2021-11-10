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

from typing import cast

import pytest
import sqlalchemy
from sqlmodel import Session, func, select

from dbgen.core.args import Const
from dbgen.core.generator import Generator
from dbgen.core.metadata import GeneratorEntity, GeneratorRunEntity, RunEntity
from dbgen.core.node.query import BaseQuery
from dbgen.core.node.transforms import PyBlock
from tests.example.full_model import Parent
from tests.example_functions import binary_lambda


@pytest.mark.database
def test_meta_registry(connection, recreate_meta):
    """assert creating metadb doesn't create Entity DBs"""
    with Session(connection) as sess:
        sess = cast(Session, sess)
        result = sess.exec(select(RunEntity.id)).one_or_none()
        assert result is None
        with pytest.raises(sqlalchemy.exc.ProgrammingError):
            sess.exec(select(Parent))


@pytest.mark.database
def test_run_insertion_and_update(connection, recreate_meta):
    run_1 = RunEntity(status="initialized")

    with Session(connection) as sess:
        sess.add(run_1)
        sess.commit()
        sess.refresh(run_1)
        assert run_1.id == 1
        run_1.status = "failed"
        sess.commit()
    with Session(connection) as sess:
        run_id, status = sess.exec(select(RunEntity.id, RunEntity.status)).one_or_none()
        assert run_id == 1
        assert status == "failed"
        run_2 = RunEntity(status="initialized")
        sess.add(run_2)
        run_count = sess.exec(select(func.count(RunEntity.id))).one()
        assert run_count == 2
        sess.refresh(run_2)
        assert run_2.id == 2


query = BaseQuery(
    outputs=["label"],
    query="select 1 as label;",
    dependency={"tables_needed": {"test"}},
)
pb = PyBlock(inputs=[Const(val=1), Const(val=2)], function=binary_lambda)
generators = [
    Generator(name="test"),
    Generator(name="test_gen", tags=["a", "b", "c"]),
    Generator(
        name="test_gen",
        transforms=[pb],
    ),
    Generator(
        name="test_gen",
        transforms=[pb],
        loads=[Parent.load(insert=True, label=pb["out"])],
    ),
    Generator(
        name="test_gen",
        extract=query,
        transforms=[pb],
        loads=[Parent.load(insert=True, label=pb["out"])],
    ),
]


@pytest.mark.database
@pytest.mark.parametrize("gen", generators)
def test_gen_insertion(connection, recreate_meta, gen: Generator):
    """Test that generators can be inserted into the database and deserialized upon querying."""
    gen_row = gen._get_gen_row()
    with Session(connection) as sess:
        sess.merge(gen_row)
        gen_count = sess.exec(select(func.count(GeneratorEntity.id))).one_or_none()
        assert gen_count == 1
        gen_dict = sess.exec(select(GeneratorEntity.gen_json)).one_or_none()
        query_gen = Generator.deserialize(gen_dict)
        assert query_gen._id_dict() == gen._id_dict()
        assert query_gen.hash == gen.hash


@pytest.mark.database
def test_gen_run_insertion(connection, recreate_meta):
    gen = generators[0]._get_gen_row()
    run = RunEntity()
    with Session(connection) as sess:
        sess.add(run)
        sess.add(gen)
        sess.commit()
        sess.refresh(run)
        gen_run = GeneratorRunEntity(run_id=run.id, generator_id=gen.id)
        sess.add(gen_run)
        sess.commit()
