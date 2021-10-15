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
from sqlalchemy.engine import Engine
from sqlmodel import Session, func, select

import tests.example.entities as entities
from dbgen.core.args import Const
from dbgen.core.entity import EntityId
from dbgen.core.func import Import
from dbgen.core.generator import Generator
from dbgen.core.load import Load
from dbgen.core.metadata import RunEntity
from dbgen.core.query import BaseQuery
from dbgen.core.transforms import PyBlock


def transform_func(x):
    return f"{x}-child"


@pytest.fixture(scope='function')
def basic_generator() -> Generator:
    Parent = entities.Parent
    Child = entities.Child
    select_stmt = select(Parent.label)
    query = BaseQuery.from_select_statement(select_stmt)
    assert isinstance(query.hash, str)
    pyblock = PyBlock(function=transform_func, inputs=[query["label"]], outputs=["newnames"])

    load = Child.load(insert=True, label=pyblock["newnames"], type=Const("child_type"))
    assert isinstance(load.hash, str)
    gen = Generator(name="test", extract=query, transforms=[pyblock], loads=[load])
    return gen


def test_basic_graph_sort(basic_generator: Generator):
    """Ensure a simple Query->PyBlock->Load is sorted correctly."""
    graph = basic_generator._computational_graph()
    assert len(graph) == 3
    sorted_nodes = basic_generator._sort_graph()
    query, transform, load = sorted_nodes
    assert isinstance(query, BaseQuery)
    assert isinstance(transform, PyBlock)
    assert isinstance(load, Load)


def test_basic_graph_in_place(basic_generator: Generator):
    """Ensure that changes to the output of ._sort_graph() are in place and affect the generator as well."""
    query, transform, load = basic_generator._sort_graph()
    assert isinstance(load, Load)
    load.run({transform.hash: {"newnames": ("1", "2")}})
    assert load._output == basic_generator._sorted_loads()[0]._output
    assert isinstance(query, BaseQuery)
    query.outputs.append("test")
    assert basic_generator.extract == query
    assert isinstance(transform, PyBlock)
    import_to_add = Import(lib="numpy", lib_alias="np")
    transform.env.imports.append(import_to_add)
    assert basic_generator.transforms[0] == transform
    assert basic_generator.transforms[0].env.imports == [import_to_add]


def test_sorted_loads():
    """Shuffle around the loads and make sure sorted_loads still works."""
    val = Const("test")
    gp_load = entities.GrandParent.load(label=val, type=val)
    u_load = entities.Parent.load(label=val, type=Const("uncle"), grand_parent_id=gp_load)
    p_load = entities.Parent.load(label=val, type=val, grand_parent_id=gp_load)
    c_load = entities.Child.load(label=val, type=val, parent_id=p_load, uncle_id=u_load)
    loads = [gp_load, c_load, p_load, u_load]
    for _ in range(10):
        shuffle(loads)
        gen = Generator(name="test", loads=loads)
        assert gen._sorted_loads() == [
            gp_load,
            *sorted((u_load, p_load), key=lambda x: x.hash),
            c_load,
        ]


@pytest.mark.skip
def test_no_extractor(sql_engine: Engine, raw_connection):
    """Shuffle around the loads and make sure sorted_loads still works."""
    entities.Parent.metadata.create_all(sql_engine)
    pyblock = PyBlock(function=transform_func, inputs=[Const("test")], outputs=["newnames"])
    p_load = entities.GrandParent.load(insert=True, label=pyblock["newnames"], type=Const("gp_type"))
    gen = Generator(name="test", transforms=[pyblock], loads=[p_load])
    gen.run(sql_engine)

    with Session(sql_engine) as session:
        session = cast(Session, session)
        statement = select(entities.GrandParent).where(entities.GrandParent.label == "test-child")
        result = session.exec(statement)
        assert result.one()


def test_dumb_extractor(connection, sql_engine, recreate_meta):
    class User(EntityId, table=True):
        __identifying__ = {"label"}
        label: str
        new_label: Optional[str] = None

    User.metadata.create_all(connection)
    num_users = 100
    sess = Session(connection)
    users = [User(label=f"user_{i}") for i in range(num_users)]
    for user in users:
        sess.add(user)
    count = sess.exec(select(func.count(User.id))).one()
    assert count == num_users
    connection.commit()
    statement = select(User.id, User.label)
    query = BaseQuery.from_select_statement(statement)
    assert query.get_row_count(connection=connection) == num_users
    pyblock = PyBlock(function=transform_func, inputs=[query["label"]])
    u_load = User.load(user=query["id"], new_label=pyblock["out"])
    run = RunEntity()
    sess.add(run)
    sess.commit()
    sess.refresh(run)
    gen = Generator(
        name="test",
        extract=query,
        transforms=[pyblock],
        loads=[u_load],
        batch_size=10000,
    )
    connection.commit()
    gen.run(sql_engine, sql_engine, run_id=run.id, ordering=0)


def get_color(node):
    from dbgen.core.extract import Extract
    from dbgen.core.generator import Generator
    from dbgen.core.load import Load
    from dbgen.core.transforms import Transform

    if isinstance(node, Transform):
        return "red"
    elif isinstance(node, Load):
        return "green"
    elif isinstance(node, Extract):
        return "blue"
    elif isinstance(node, Generator):
        return "green"
    return "gray"


def get_label(node):
    from dbgen.core.extract import Extract
    from dbgen.core.generator import Generator
    from dbgen.core.load import Load
    from dbgen.core.transforms import Transform

    if isinstance(node, Transform):
        return str(node)
    elif isinstance(node, Load):
        return node.load_entity.name
    elif isinstance(node, Extract):
        return "Query"
    elif isinstance(node, Generator):
        return node.name
    return "None"
