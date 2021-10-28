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

"""Utilities for converting coercing types."""
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum, unique
from json import dumps
from typing import Any, Callable, Mapping, Optional, Type, Union
from uuid import UUID

import sqlalchemy.dialects.postgresql as sa_postgres
import sqlalchemy.types as sa_types

# TODO convert this enum to a class that allows for easy conversion


@unique
class SQLTypeEnum(str, Enum):
    """Accepted SQLAlchemy Types"""

    FLOAT = 'float'
    NUMERIC = 'numeric'
    INTEGER = 'integer'
    BIGINTEGER = 'biginteger'
    BOOLEAN = 'boolean'
    DATE = 'date'
    DATETIME = 'datetime'
    INTERVAL = 'interval'
    TIME = 'time'
    ENUM = 'enum'
    LARGEBINARY = 'largebinary'
    TEXT = 'text'
    UUID = 'uuid'
    JSONB = 'jsonb'
    JSON = 'json'
    ARRAY = 'array'


SQL_TYPE_SET = {val.value for _, val in SQLTypeEnum.__members__.items()}


# Map str -> SQLType
SQL_TYPE_STR_TO_SQL_TYPE: Mapping[SQLTypeEnum, Type[sa_types.TypeEngine]] = {
    SQLTypeEnum.FLOAT: sa_types.Float,
    SQLTypeEnum.NUMERIC: sa_types.Numeric,
    SQLTypeEnum.INTEGER: sa_types.Integer,
    SQLTypeEnum.BIGINTEGER: sa_types.BigInteger,
    SQLTypeEnum.BOOLEAN: sa_types.Boolean,
    SQLTypeEnum.DATE: sa_types.Date,
    SQLTypeEnum.DATETIME: sa_types.DateTime,
    SQLTypeEnum.INTERVAL: sa_types.Interval,
    SQLTypeEnum.TIME: sa_types.Time,
    SQLTypeEnum.ENUM: sa_types.Enum,
    SQLTypeEnum.LARGEBINARY: sa_types.LargeBinary,
    SQLTypeEnum.TEXT: sa_types.Text,
    SQLTypeEnum.UUID: sa_postgres.UUID,
    SQLTypeEnum.ARRAY: sa_postgres.ARRAY,
    SQLTypeEnum.JSON: sa_postgres.JSON,
    SQLTypeEnum.JSONB: sa_postgres.JSONB,
}
# Map SQLTYPE -> str
SQL_TYPE_TO_SQL_TYPE_STR: Mapping[Type[sa_types.TypeEngine], SQLTypeEnum] = {
    v: k for k, v in SQL_TYPE_STR_TO_SQL_TYPE.items()
}
# str -> python type
SQL_TYPE_STR_TO_PYTHON_TYPE: Mapping[SQLTypeEnum, type] = {
    SQLTypeEnum.FLOAT: float,
    SQLTypeEnum.NUMERIC: Decimal,
    SQLTypeEnum.INTEGER: int,
    SQLTypeEnum.BIGINTEGER: int,
    SQLTypeEnum.BOOLEAN: bool,
    SQLTypeEnum.DATE: date,
    SQLTypeEnum.DATETIME: datetime,
    SQLTypeEnum.INTERVAL: timedelta,
    SQLTypeEnum.TIME: time,
    SQLTypeEnum.ENUM: Enum,
    SQLTypeEnum.LARGEBINARY: bytes,
    SQLTypeEnum.TEXT: str,
    SQLTypeEnum.UUID: UUID,
    SQLTypeEnum.ARRAY: list,
    SQLTypeEnum.JSON: dict,
    SQLTypeEnum.JSONB: dict,
}
# Default PythonType -> SQLType
PYTHON_TYPE_TO_SQL_TYPE_STR: Mapping[type, SQLTypeEnum] = {
    float: SQLTypeEnum.FLOAT,
    Decimal: SQLTypeEnum.NUMERIC,
    int: SQLTypeEnum.INTEGER,
    bool: SQLTypeEnum.BOOLEAN,
    date: SQLTypeEnum.DATE,
    datetime: SQLTypeEnum.DATETIME,
    timedelta: SQLTypeEnum.INTERVAL,
    time: SQLTypeEnum.TIME,
    Enum: SQLTypeEnum.ENUM,
    bytes: SQLTypeEnum.LARGEBINARY,
    str: SQLTypeEnum.TEXT,
    UUID: SQLTypeEnum.UUID,
    list: SQLTypeEnum.ARRAY,
    dict: SQLTypeEnum.JSONB,
}

# Utilities for converting types
# Type coercion funcs
datetime_types = (datetime, time, date)
datetime_to_str = lambda x: x.isoformat() if x is not None else str(x)
timedelta_to_str = lambda x: str(x.total_seconds()) if x is not None else str(x)
basic_to_str = lambda x: str(x)
bytes_to_str = lambda x: x.decode('utf-8') if x is not None else str(x)

# TODO create central location for all json serialization (pydasher does hashing serialization)
def json_default(thing: Any) -> str:
    if isinstance(thing, datetime_types):
        return thing.isoformat()
    elif isinstance(thing, timedelta):
        return timedelta_to_str(thing)
    elif isinstance(thing, bytes):
        return bytes_to_str(thing)
    elif isinstance(thing, (Decimal, UUID)):
        return str(thing)
    raise TypeError(f"Cannot serialize object of type {type(thing)}: {thing}")


def dict_to_str(x: Optional[dict]) -> str:
    if x is None:
        return str(x)
    try:
        return dumps(x, default=json_default)
    except TypeError as exc:
        raise TypeError(
            f"Error coercing dict to string due to json serialization error. We can only load json serializable data currently."
        ) from exc


# Dummy non implementation caster for good error messaging
def not_impl(x):
    raise NotImplementedError(f"haven't found a way to coerce type {type(x)} to string :(")


# Map for converting each column to string for Postgres COPY FROM command
SQL_TYPE_TO_STR_CONVERTER = {
    # Basic Conversion
    SQLTypeEnum.FLOAT: basic_to_str,
    SQLTypeEnum.NUMERIC: basic_to_str,
    SQLTypeEnum.INTEGER: basic_to_str,
    SQLTypeEnum.BIGINTEGER: basic_to_str,
    SQLTypeEnum.BOOLEAN: basic_to_str,
    SQLTypeEnum.UUID: basic_to_str,
    SQLTypeEnum.TEXT: basic_to_str,
    # Datetime conversions
    SQLTypeEnum.DATE: datetime_to_str,
    SQLTypeEnum.DATETIME: datetime_to_str,
    SQLTypeEnum.TIME: datetime_to_str,
    SQLTypeEnum.INTERVAL: lambda x: x.total_seconds(),
    # JSON conversion
    SQLTypeEnum.JSON: dict_to_str,
    SQLTypeEnum.JSONB: dict_to_str,
    # Other
    SQLTypeEnum.LARGEBINARY: bytes_to_str,
    # Not implemented
    SQLTypeEnum.ENUM: not_impl,
    SQLTypeEnum.ARRAY: not_impl,
}


def validate_sql_type_str(sql_type_str: str):
    if sql_type_str not in SQL_TYPE_SET:
        raise TypeError(f"Invalid SQLType string {sql_type_str}")


def get_sql_type_str(type_: Union[Type[sa_types.TypeEngine], type]) -> SQLTypeEnum:
    """Convert SQLType/PythonTypes -> stringified SQLType"""
    try:
        if issubclass(type_, sa_types.TypeEngine):
            return SQL_TYPE_TO_SQL_TYPE_STR[type_]
        elif isinstance(type_, type):
            if issubclass(type_, Enum):
                type_ = Enum
            return PYTHON_TYPE_TO_SQL_TYPE_STR[type_]
    except TypeError:
        pass
    raise TypeError(f"Unknown type encountered {type_} of type {type(type_)}")


def get_column_type(sql_type_str: str) -> Type[sa_types.TypeEngine]:
    """Convert python type -> SQLType"""
    validate_sql_type_str(sql_type_str)
    return SQL_TYPE_STR_TO_SQL_TYPE[SQLTypeEnum(sql_type_str)]


def get_python_type(sql_type_str: str) -> type:
    """Converts a stringified type to its valid python constructor

    Args:
        type_str (str): A stringified type (i.e. 'str', 'int','bool')

    Raises:
        TypeError: type_str not found in the TypeEnum setting

    Returns:
        type: the constructor for the relevant python type
    """
    validate_sql_type_str(sql_type_str)
    return SQL_TYPE_STR_TO_PYTHON_TYPE[SQLTypeEnum(sql_type_str)]


def get_str_converter(sql_type_str: str) -> Callable[[Optional[Any]], str]:
    """Get the correct function for converting a given type to a str

    Args:
        type_ (Type[T]): A valid type included in the TypeEnum

    Returns:
        Callable[[T], str]: A callable that takes in a value of the given type and returns a str
    """
    validate_sql_type_str(sql_type_str)
    return SQL_TYPE_TO_STR_CONVERTER[SQLTypeEnum(sql_type_str)]
