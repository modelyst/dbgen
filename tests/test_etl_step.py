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

from random import shuffle
from typing import Optional, cast

import pytest
from sqlalchemy.future import Engine
from sqlmodel import Session, func, select

import tests.example.entities as entities
from dbgen.core.args import Constant
from dbgen.core.entity import Entity
from dbgen.core.etl_step import ETLStep
from dbgen.core.func import Import
from dbgen.core.metadata import RunEntity
from dbgen.core.node.load import Load
from dbgen.core.node.query import BaseQuery
from dbgen.core.node.transforms import PythonTransform


def transform_func(x):
    return f"{x}-child"


@pytest.fixture(scope='function')
def basic_etl_step() -> ETLStep:
    Parent = entities.Parent
    Child = entities.Child
    select_stmt = select(Parent.label)
    query = BaseQuery.from_select_statement(select_stmt)
    assert isinstance(query.hash, str)
    pyblock = PythonTransform(function=transform_func, inputs=[query["label"]], outputs=["newnames"])

    load = Child.load(insert=True, label=pyblock["newnames"], type=Constant("child_type"))
    assert isinstance(load.hash, str)
    etl_step = ETLStep(name="test", extract=query, transforms=[pyblock], loads=[load])
    return etl_step


def test_basic_graph_sort(basic_etl_step: ETLStep):
    """Ensure a simple Query->transform->Load is sorted correctly."""
    graph = basic_etl_step._computational_graph()
    assert len(graph) == 3
    sorted_nodes = basic_etl_step._sort_nodes()
    query, transform, load = sorted_nodes
    assert isinstance(query, BaseQuery)
    assert isinstance(transform, PythonTransform)
    assert isinstance(load, Load)


def test_basic_graph_in_place(basic_etl_step: ETLStep):
    """Ensure that changes to the output of ._sort_graph() are in place and affect the ETLStep as well."""
    query, transform, load = basic_etl_step._sort_nodes()
    assert isinstance(load, Load)
    load.run({transform.hash: {"newnames": ("1", "2")}})
    assert load._output == basic_etl_step._sorted_loads()[0]._output
    assert isinstance(query, BaseQuery)
    query.outputs.append("test")
    assert basic_etl_step.extract == query
    assert isinstance(transform, PythonTransform)
    import_to_add = Import(lib="numpy", lib_alias="np")
    transform.env.imports.append(import_to_add)
    assert basic_etl_step.transforms[0] == transform
    assert basic_etl_step.transforms[0].env.imports == [import_to_add]


def test_sorted_loads():
    """Shuffle around the loads and make sure sorted_loads still works."""
    val = Constant("test")
    gp_load = entities.GrandParent.load(label=val, type=val)
    u_load = entities.Parent.load(label=val, type=Constant("uncle"), grand_parent_id=gp_load)
    p_load = entities.Parent.load(label=val, type=val, grand_parent_id=gp_load)
    c_load = entities.Child.load(label=val, type=val, parent_id=p_load, uncle_id=u_load)
    loads = [gp_load, c_load, p_load, u_load]
    for _ in range(10):
        shuffle(loads)
        etl_step = ETLStep(name="test", loads=loads)
        assert etl_step._sorted_loads() == [
            gp_load,
            *sorted((u_load, p_load), key=lambda x: x.hash),
            c_load,
        ]


@pytest.mark.skip
def test_no_extractor(sql_engine: Engine):
    """Shuffle around the loads and make sure sorted_loads still works."""
    entities.Parent.metadata.create_all(sql_engine)
    pyblock = PythonTransform(function=transform_func, inputs=[Constant("test")], outputs=["newnames"])
    p_load = entities.GrandParent.load(insert=True, label=pyblock["newnames"], type=Constant("gp_type"))
    etl_step = ETLStep(name="test", transforms=[pyblock], loads=[p_load])
    etl_step.run(sql_engine)

    with Session(sql_engine) as session:
        session = cast(Session, session)
        statement = select(entities.GrandParent).where(entities.GrandParent.label == "test-child")
        result = session.exec(statement)
        assert result.one()


@pytest.mark.database
def test_dumb_extractor(connection, sql_engine, clear_registry, recreate_meta):
    class TestUser(Entity, table=True):
        __identifying__ = {"label"}
        label: Optional[str]
        new_label: Optional[str] = None

    TestUser.__table__.drop(connection, checkfirst=True)
    TestUser.metadata.create_all(connection)
    num_users = 100
    with Session(connection) as sess:
        users = [TestUser(label=f"user_{i}") for i in range(num_users)]
        user_le = TestUser._get_load_entity()
        for user in users:
            user.id = user_le._get_hash(user.dict())
            sess.add(user)
        count = sess.exec(select(func.count(TestUser.id))).one()
        assert count == num_users
        connection.commit()
    statement = select(TestUser.id, TestUser.label)
    query = BaseQuery.from_select_statement(statement)
    query.set_connection(connection, None)
    assert query.length() == num_users
    pyblock = PythonTransform(function=transform_func, inputs=[query["label"]])
    u_load = TestUser.load(id=query["id"], new_label=pyblock["out"])
    run = RunEntity()
    sess.add(run)
    sess.commit()
    sess.refresh(run)
    etl_step = ETLStep(
        name="test",
        extract=query,
        transforms=[pyblock],
        loads=[u_load],
        batch_size=10000,
    )
    connection.commit()
    etl_step.run(sql_engine, sql_engine, run_id=run.id, ordering=0)
    # Assert the run was succesful by counting the number of users where new_label is '{label}-child'
    with Session(sql_engine) as sess:
        current_count = sess.exec(
            select(func.count(TestUser.id)).where(TestUser.label + '-child' == TestUser.new_label)
        ).one()
        assert current_count == num_users
