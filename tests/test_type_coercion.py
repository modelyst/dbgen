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

from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from sqlalchemy.orm import registry

from dbgen.core.entity import Entity
from dbgen.utils.type_coercion import (
    SQLTypeEnum,
    get_column_type,
    get_python_type,
    get_sql_type_str,
    get_str_converter,
)
from tests.strategies import json_strat

type_registry = registry()


class TypeEntity(Entity, all_id=True, table=True, registry=type_registry):
    int_val: int
    str_val: str
    bool_val: bool
    float_val: float
    bytes_val: bytes
    json_val: dict
    uuid_val: UUID
    datetime_val: datetime
    date_val: date
    time_val: time
    decimal_val: Decimal


byte_strat = st.text().map(lambda x: x.encode())
type_entity_strat = st.builds(TypeEntity, bytes_val=byte_strat)


def test_type_load_entity():
    TypeEntity._get_load_entity()


@given(type_entity_strat)
def test_type_entity(type_entity):
    raw_data = type_entity.dict(exclude={'id': True})
    new_entity = TypeEntity(**raw_data)
    assert type_entity._id_dict() == new_entity._id_dict()
    assert type_entity.uuid == new_entity.uuid


def test_type_coercion():
    for val in SQLTypeEnum._member_map_.values():
        get_python_type(val)


def test_bad_type():
    bad_type = 'nonexistent_type'
    with pytest.raises(TypeError):
        get_column_type(bad_type)


@given(json_strat(allow_lists=False))
def test_cast_to_str(thing: dict):
    assume(thing is not None)
    type_ = type(thing)
    str_caster = get_str_converter(get_sql_type_str(type_))
    assert isinstance(str_caster(thing), str)


def test_bad_type_to_str():
    with pytest.raises(TypeError):
        get_str_converter(set)
