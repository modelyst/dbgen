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

from importlib import reload

import pytest
from hypothesis import HealthCheck, given, settings
from pydantic import ValidationError
from sqlalchemy import func
from sqlmodel import Session, select

import tests.example.entities as entities
from dbgen.core.args import Arg, Const
from dbgen.core.dependency import Dependency
from dbgen.core.load import Load, LoadEntity
from dbgen.utils.lists import broadcast
from tests.strategies import (
    basic_insert_load_strat,
    basic_load_strat,
    basic_update_load_strat,
    recursive_load_strat,
)


@pytest.fixture
def simple_load():
    load_entity = LoadEntity(
        name="Test",
        primary_key_name="id",
        identifying_attributes={"key_1": "str", "key_2": "int"},
    )
    load = Load(
        load_entity=load_entity,
        inputs={"key_1": Const("key_1_val"), "key_2": Const("key_2_val")},
        insert=True,
    )
    return load


def test_build_io_obj(clear_registry, debug_logger, simple_load, connection, raw_connection):

    reload(entities)
    Child = entities.Child
    Parent = entities.Parent
    parent_load = entities.Parent.load(
        insert=True,
        label=Arg(key="pyblock", name="label"),
        type=Const("parent_type"),
    )
    child_load = Child.load(
        insert=True,
        label=Const("child_label"),
        type=Const("child_type"),
        parent_id=parent_load,
    )
    Child.metadata.create_all(connection)
    connection.commit()
    n_rows = 1000
    namespace_rows = [{"pyblock": {"label": [i for i in range(n_rows)]}}]
    for row in namespace_rows:
        for load in (parent_load, child_load):
            row[load.hash] = load.run(row)
    parent_load.load(raw_connection, gen_id=parent_load.hash)
    child_load.load(raw_connection, gen_id=parent_load.hash)
    # Query the Parent/child table and ensure number of rows is correct
    with Session(connection) as session:
        out = session.execute(select(func.count(Parent.id))).scalar()
        assert out == n_rows
        out = session.execute(select(func.count(Child.id))).scalar()
        assert out == 1
        out = session.execute(select(func.count(Child.parent_id))).scalar()
        assert out == 1
    Child.metadata.drop_all(connection)


# def test_basic_load(raw_connection, session):
#     number_of_samples = 1000
#     pyblock = PyBlock(lambda: [i for i in range(1000)])
#     sample_load = Sample.load(label=pyblock["out"], type=Const("jcap"), insert=True)
#     assert isinstance(sample_load, OldLoad)
#     assert sample_load.insert
#     universe = get_universe([Sample])
#     sample_load.act(
#         raw_connection, universe, rows=[{pyblock.hash: pyblock({})}], gen_name="test"
#     )
#     raw_connection.commit()
#     with raw_connection.cursor() as curs:
#         curs.execute("select count(1) from sample;")
#         (x,) = curs.fetchone()
#         assert x == number_of_samples
#     sample = session.exec(select(Sample).where(Sample.label == "4")).one()
#     assert isinstance(sample, Sample) and sample.label == "4"


# def test_invalid_args():
#     with pytest.raises(exc.DBgenInvalidArgument):
#         Sample.load(bad_arg=1, type=Const("jcap"))


# def test_missing_id():
#     with pytest.raises(exc.DBgenMissingInfo):
#         Sample.load(type=Const("jcap"))


# def test_extra_kwarg():
#     with pytest.raises(exc.DBgenInvalidArgument):
#         Sample.load(label=Const(1), bad_arg=Const(1), type=Const("jcap"))


# def test_const_fks():
#     SampleCollection.load(sample_id=Const(None), collection_id=Const(None))
#     process_load = Process.load(
#         machine_name=Const("hte-jcap-1"),
#         ordering=Const(0),
#         timestamp=Const(datetime.now()),
#         process_detail_id=Const(None),
#     )
#     pb = PyBlock(lambda: 1)
#     sample_load = Sample.load(label=pb["out"], type=Const(1))
#     sample_process_insert = SampleProcess.load(
#         sample_id=sample_load, process_id=process_load, insert=True
#     )
#     assert sample_process_insert.insert


def test_broadcast():
    assert list(broadcast(*[[1], (1, 2), ("test",)])) == [
        (1, 1, "test"),
        (1, 2, "test"),
    ]
    assert list(broadcast(*[[1], [1, 2], ["a", "b"]])) == [(1, 1, "a"), (1, 2, "b")]
    assert list(broadcast(*[[1, 2], (1, 2)])) == [(1, 1), (2, 2)]
    assert list(broadcast(*[[1], (1, 2)])) == [(1, 1), (1, 2)]

    dict_input = {"a": [1], "b": (1, 2, 3), "c": ("test",)}
    assert list(broadcast(*dict_input.values())) == [
        (1, 1, "test"),
        (1, 2, "test"),
        (1, 3, "test"),
    ]
    with pytest.raises(ValueError):
        list(broadcast(*[[1, 2, 3], (1, 2)]))


def test_load_validation():
    good_kwargs = {
        "load_entity": LoadEntity(name="test", primary_key_name="id"),
        "inputs": {"label": Const("test")},
        "primary_key": Const(None),
        "insert": False,
    }
    load = Load(**good_kwargs)
    assert load.primary_key == Const(None)
    with pytest.raises(ValidationError):
        Load(**{**good_kwargs, "primary_key": 1})
    with pytest.raises(ValidationError):
        Load(**{**good_kwargs, "inputs": {"label": 1}})
    with pytest.raises(ValidationError):
        Load(**{**good_kwargs, "insert": True})
    with pytest.raises(ValidationError):
        Load(**{**good_kwargs, "primary_key": Const(2)})


@given(basic_update_load_strat)
def test_update_load_hypo(instance: Load):
    assert isinstance(instance, Load)


@given(basic_insert_load_strat)
def test_insert_load_hypo(instance: Load):
    assert isinstance(instance, Load)


@given(basic_load_strat)
def test_load_hypo(instance: Load):
    assert isinstance(instance, Load)


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(recursive_load_strat())
def test_recursive_load_hypo(instance: Load):
    assert isinstance(instance, Load)


def test_load_dependency(simple_load):
    dep = simple_load._get_dependency()
    assert isinstance(dep, Dependency)
    assert {"Test"} == dep.tables_yielded
    assert {
        "Test.key_1",
        "Test.key_2",
    } == dep.columns_yielded


def test_load_dependency_no_insert(simple_load):

    reload(entities)
    Parent = entities.Parent

    parent_load = Parent.load(
        insert=False,
        type=Const("test_type"),
        label=Const("test_label"),
        non_id=Const(2),
    )
    dep = parent_load._get_dependency()
    full_name = parent_load.load_entity.full_name
    assert isinstance(dep, Dependency)
    assert set() == dep.tables_yielded
    assert {f"{full_name}.{col}" for col in ("non_id",)} == dep.columns_yielded
    assert {f"{full_name}.{col}" for col in ("type", "label")} == dep.columns_needed
    assert {full_name} == dep.tables_needed
