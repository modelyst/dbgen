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
from enum import Enum
from inspect import isclass
from uuid import UUID

from sqlalchemy.orm import registry

from dbgen.core.entity import Entity
from dbgen.utils.type_coercion import column_registry

type_registry = registry()


class Colors(str, Enum):
    RED = 'red'
    BLUE = 'blue'


class BaseTypeEntity(Entity):
    int_val: int
    str_val: str = 'hello world'
    bool_val: bool


class TypeEntity(BaseTypeEntity, all_id=True, table=True, registry=type_registry):
    __tablename__ = 'type_entity'
    float_val: float
    bytes_val: bytes
    json_val: dict
    uuid_val: UUID
    datetime_val: datetime
    date_val: date
    time_val: time
    decimal_val: Decimal


def test_column_type_map():
    for name, field in TypeEntity.__fields__.items():
        col = getattr(TypeEntity, name)
        datatype = column_registry.get_from_python_type(field.type_)
        expected_type = datatype.get_column_type()
        actual_type = col.type
        assert (isclass(expected_type) and isinstance(actual_type, expected_type)) or type(
            actual_type
        ) == type(expected_type)


# byte_strat = st.text().map(lambda x: x.encode())
# type_entity_strat = st.builds(TypeEntity, bytes_val=byte_strat)


# def test_type_load_entity():
#     TypeEntity._get_load_entity()
#     assert isinstance(TypeEntity.int_val.type, get_column_type(SQLTypeEnum.INTEGER))
#     assert isinstance(TypeEntity.str_val.type, get_column_type(SQLTypeEnum.TEXT))
#     assert isinstance(TypeEntity.bool_val.type, get_column_type(SQLTypeEnum.BOOLEAN))
#     assert isinstance(TypeEntity.float_val.type, get_column_type(SQLTypeEnum.FLOAT))


# @given(type_entity_strat)
# def test_type_entity(type_entity):
#     raw_data = type_entity.dict(exclude={'id': True})
#     new_entity = TypeEntity(**raw_data)
#     assert type_entity._id_dict() == new_entity._id_dict()
#     assert type_entity.uuid == new_entity.uuid


# def test_type_coercion():
#     for val in SQLTypeEnum._member_map_.values():
#         get_python_type(val)


# def test_bad_type():
#     bad_type = 'nonexistent_type'
#     with pytest.raises(TypeError):
#         get_column_type(bad_type)


# @given(json_strat(allow_lists=False))
# def test_cast_to_str(thing: dict):
#     assume(thing is not None)
#     type_ = type(thing)
#     str_caster = get_str_converter(get_sql_type_str(type_))
#     assert isinstance(str_caster(thing), str)


# def test_bad_type_to_str():
#     with pytest.raises(TypeError):
#         get_str_converter(set)


# sql_type_str_answers = [
#     (UUID, SQLTypeEnum.UUID),
#     (Colors, SQLTypeEnum.ENUM),
#     (int, SQLTypeEnum.INTEGER),
#     (str, SQLTypeEnum.TEXT),
#     (datetime, SQLTypeEnum.TIMESTAMP),
#     (date, SQLTypeEnum.DATE),
#     (timedelta, SQLTypeEnum.INTERVAL),
#     (time, SQLTypeEnum.TIME),
#     (bytes, SQLTypeEnum.LARGEBINARY),
#     (dict, SQLTypeEnum.JSONB),
#     (Decimal, SQLTypeEnum.NUMERIC),
# ]
# list_map_answers = [
#     (str, SQLTypeEnum.TEXT_ARRAY),
#     (int, SQLTypeEnum.INTEGER_ARRAY),
#     (bool, SQLTypeEnum.BOOLEAN_ARRAY),
#     (dict, SQLTypeEnum.JSONB_ARRAY),
#     (float, SQLTypeEnum.FLOAT_ARRAY),
# ]
# get_id = lambda answers: (k.__name__ for k, _ in answers)


# @pytest.mark.parametrize('python_type,answer', sql_type_str_answers, ids=get_id(sql_type_str_answers))
# def test_get_sql_type_str_basic(python_type: type, answer: SQLTypeEnum):
#     assert get_sql_type_str(python_type) == answer


# @pytest.mark.parametrize('python_type,answer', sql_type_str_answers, ids=get_id(sql_type_str_answers))
# def test_get_sql_type_str_optional(python_type, answer: SQLTypeEnum):
#     assert get_sql_type_str(Optional[python_type]) == answer  # type: ignore


# @pytest.mark.parametrize(
#     'python_type,answer', list_map_answers, ids=(k.__name__ for k, _ in list_map_answers)
# )
# def test_get_sql_type_str_list(python_type: type, answer: SQLTypeEnum):
#     assert get_sql_type_str(List[python_type]) == answer  # type: ignore


# # @pytest.mark.parametrize('column_type,answer', column_type, ids=(k.__name__ for k, _ in list_map_answers))

# def test_get_sql_type_from_col(column_type: type, answer: SQLTypeEnum):
#     assert get_sql_type_str(sa_types.ARRAY(sa_types.INTEGER)) == SQLTypeEnum.INTEGER_ARRAY  # type: ignore


# def test_psycopg2_string_casting():
#     assert list_to_str([1, 2, 3], int) == '{1,2,3}'
#     assert list_to_str(["1", "2", "3"], str) == "{'1','2','3'}"
#     assert list_to_str([True, False, False], bool) == "{true,false,false}"
#     assert list_to_str([1.0, 3.0, 1.0], float) == "{1.0,3.0,1.0}"
#     assert list_to_str(["a", "b", "c"], str) == "{'a','b','c'}"
#     assert list_to_str(["c", "b", "c"], str) == "{'c','b','c'}"


# @pytest.fixture(scope='module')
# def create_dummy_table():
#     connection = connect(config.main_dsn)
#     with connection.cursor() as curs:
#         curs.execute(
#             'Create table if not exists test_psycopg2_string_casting_hypo(id serial primary key, array_col text[]);'
#         )
#     connection.commit()
#     yield connection
#     # with connection.cursor() as curs:
#     #     curs.execute('drop table test_psycopg2_string_casting_hypo;')
#     connection.commit()
#     connection.close()


# @given(st.text(printable), st.text(printable), st.text(printable))
# def test_psycopg2_string_casting_hypo(
#     create_dummy_table,
#     a,
#     b,
#     c,
# ):
#     io_obj = StringIO()
#     list_val = [a, b, c]
#     list_str = list_to_str(list_val, str)
#     print(list_str)
#     io_obj.write(list_str)
#     io_obj.seek(0)
#     try:
#         with create_dummy_table.cursor() as curs:
#             curs.execute('begin')
#             curs.copy_from(io_obj, 'test_psycopg2_string_casting_hypo', columns=['array_col'], sep='\t')
#             create_dummy_table.commit()
#     finally:
#         create_dummy_table.rollback()

# valid_python_types = column_registry._default_python_type_registry.keys()


# @pytest.mark.parametrize('python_type', valid_python_types)
# def test_unpack_type_hints(python_type):
#     assert unpack_type_hint(python_type) == python_type


# @pytest.mark.parametrize('python_type', valid_python_types)
# def test_unpack_type_hints_optional(python_type):
#     assert unpack_type_hint(Optional[python_type]) == python_type


# @pytest.mark.parametrize('python_type', valid_python_types)
# def test_unpack_type_hints_list(python_type):
#     assert unpack_type_hint(List[python_type]) == python_type
