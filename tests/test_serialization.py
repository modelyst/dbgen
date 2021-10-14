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

from datetime import date, datetime
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st

from dbgen.core.base import Base
from dbgen.core.query import BaseQuery, Dependency
from tests.strategies import (
    arg_like_strat,
    env_strat,
    func_strat,
    import_strat,
    load_entity_strat,
    pyblock_strat,
    recursive_load_strat,
)


def reverse_serial(thing: Base):
    assert thing == thing.parse_obj(thing.dict())
    assert thing == thing.parse_raw(thing.json())
    assert thing.hash == thing.parse_raw(thing.json()).hash


@given(st.builds(BaseQuery, dependency=st.builds(Dependency)))
def test_query(instance):
    assert instance
    reverse_serial(instance)


@given(env_strat)
def test_serial_env(instance):
    assert instance
    reverse_serial(instance)


@given(import_strat)
def test_serial_import(instance):
    assert instance
    reverse_serial(instance)


@given(arg_like_strat)
def test_serial_arglike(instance):
    assert instance
    reverse_serial(instance)


@given(func_strat)
def test_serial_func(instance):
    assert instance
    reverse_serial(instance)


@given(pyblock_strat)
def test_pyblock(instance):
    assert instance
    reverse_serial(instance)


@given(st.builds(Dependency))
def test_dependency(instance):
    assert instance
    reverse_serial(instance)


@given(load_entity_strat(3))
def test_load_entity(instance):
    assert instance
    reverse_serial(instance)


@given(recursive_load_strat())
def test_recursive_load_strat(instance):
    assert instance
    reverse_serial(instance)


class DummyClass(Base):
    key_1: str
    key_2: int
    key_3 = "test"
    ex_key: str

    _hashexclude_ = {"ex_key"}


class DummyClassNoExclude(Base):
    key_1: str
    key_2: int
    ex_key: str


def test_exclude():
    dummy_1 = DummyClass(key_1="1", key_2=2, ex_key="1")
    dummy_2 = DummyClass(key_1="1", key_2=2, ex_key="2")
    dummy_3 = DummyClass(key_1="1", key_2=0, ex_key="2")
    dummy_4 = DummyClassNoExclude(key_1="1", key_2=0, ex_key="2")

    assert dummy_1 == dummy_2
    assert dummy_1.dict() != dummy_2.dict()
    assert dummy_1._id_dict() == dummy_2._id_dict()
    assert dummy_1 != dummy_3
    assert dummy_2 != dummy_3
    assert type(dummy_4) != type(dummy_1)
    assert "ex_key" in dummy_4._id_dict()


class DummyClass2(Base):
    time: datetime
    date: date
    id_val: UUID


@given(st.builds(DummyClass2))
def test_non_builtin_serialization_reverse(instance: DummyClass2):
    reverse_serial(instance)
