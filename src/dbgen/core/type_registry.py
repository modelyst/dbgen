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

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, Optional, Set, Union
from uuid import UUID

import sqlalchemy.dialects.postgresql as postgres_types
import sqlalchemy.types as sa_types
import sqlmodel.sql.sqltypes as sqlmodel_types
from psycopg import adapters

from dbgen.core.base import Base
from dbgen.utils.typing import COLUMN_TYPE


class DataType(Base):
    """A container for mapping SQLAlchemy Types, python types, and postgres types."""

    name: str
    postgres_name: Optional[str] = None
    type_hint: type
    python_type: type
    column_type: COLUMN_TYPE
    _hashinclude_ = {'name'}

    class Config:
        """Pydantic doc string"""

        arbitrary_types_allowed = True

    def get_column_type(self) -> COLUMN_TYPE:
        return self.column_type

    def get_array_column_type(self) -> sa_types.ARRAY[sa_types.TypeEngine]:
        return sa_types.ARRAY(self.get_column_type())

    def get_python_type(self) -> type:
        return self.python_type

    @property
    def oid(self) -> int:
        key = self.postgres_name or self.name
        return adapters.types.get_oid(key)

    @property
    def array_oid(self) -> int:
        key = self.postgres_name or self.name
        return adapters.types.get_oid(f'{key}[]')

    @property
    def type_name(self):
        return self.postgres_name or self.name


class ColumnTypeRegistry:
    """A registry for all the datatypes in the model."""

    _registry: Dict[Union[str, int, COLUMN_TYPE], DataType]
    _default_python_type_registry: Dict[type, DataType]
    _python_type_registry: Dict[type, Set[DataType]]

    def __init__(self):
        self.clear()

    def clear(self):
        self._registry = {}
        self._default_python_type_registry = {}
        self._python_type_registry = defaultdict(set)

    def add(self, data_type: DataType) -> None:
        self._registry[data_type.name] = data_type
        self._registry[data_type.oid] = data_type
        self._registry[data_type.array_oid] = data_type
        self._registry[data_type.get_column_type()] = data_type
        self._registry[data_type.get_array_column_type()] = data_type
        # register default value as first value seen by registry
        if data_type.python_type not in self._python_type_registry:
            self._default_python_type_registry[data_type.python_type] = data_type
        self._python_type_registry[data_type.python_type].add(data_type)

    def __getitem__(self, key: Union[str, int, COLUMN_TYPE]) -> DataType:
        if isinstance(key, str):
            if key.endswith('[]'):
                key = key[:-2]
        try:
            if isinstance(key, sa_types.ARRAY):
                key = key.item_type
            if isinstance(key, sa_types.TypeEngine):
                key = type(key)
            return self._registry[key]
        except KeyError:
            raise KeyError(f"Cannot find key {key!r} in ColumnType Registry.")

    def get(self, key) -> Optional[DataType]:
        try:
            return self[key]
        except KeyError:
            return None

    def get_from_python_type(self, type_: type) -> DataType:
        if type_ in self._default_python_type_registry:
            return self._default_python_type_registry[type_]

        if getattr(type_, '__mro__', None):
            for type_curr in type_.__mro__:
                if type_curr in self._default_python_type_registry:
                    return self._default_python_type_registry[type_curr]

        if type_ not in self._default_python_type_registry:
            raise ValueError(f"Cannot find python type {type_!r} in registry")
        return self._default_python_type_registry[type_]

    def overwrite_default(self, data_type: DataType) -> None:
        """Overwrite the default DataType for a given python type."""
        self._default_python_type_registry[data_type.python_type] = data_type


# Order Matters here as the when two datatypes share the same python type the first one is selected
COLUMN_TYPES = [
    DataType(
        name='autostring',
        postgres_name='text',
        type_hint=str,
        python_type=str,
        column_type=sqlmodel_types.AutoString,
    ),
    DataType(name='text', type_hint=str, python_type=str, column_type=sa_types.Text),
    DataType(name='int4', type_hint=int, python_type=int, column_type=sa_types.Integer),
    DataType(name='int8', type_hint=int, python_type=int, column_type=sa_types.BigInteger),
    DataType(name='float8', type_hint=float, python_type=float, column_type=sa_types.Float),
    DataType(name='bool', type_hint=bool, python_type=bool, column_type=sa_types.Boolean),
    DataType(name='date', type_hint=date, python_type=date, column_type=sa_types.Date),
    DataType(
        name='timestamptz',
        type_hint=datetime,
        python_type=datetime,
        column_type=sa_types.DateTime(timezone=True),
    ),
    DataType(name='timestamp', type_hint=datetime, python_type=datetime, column_type=sa_types.DateTime),
    DataType(name='interval', type_hint=timedelta, python_type=timedelta, column_type=sa_types.Interval),
    DataType(name='time', type_hint=time, python_type=time, column_type=sa_types.Time),
    DataType(name='bytea', type_hint=bytes, python_type=bytes, column_type=sa_types.LargeBinary),
    DataType(
        name='guid', postgres_name='uuid', type_hint=UUID, python_type=UUID, column_type=sqlmodel_types.GUID
    ),
    DataType(name='uuid', type_hint=UUID, python_type=UUID, column_type=postgres_types.UUID),
    DataType(name='json', type_hint=dict, python_type=dict, column_type=sa_types.JSON),
    DataType(
        name='pg_json',
        postgres_name='json',
        type_hint=dict,
        python_type=dict,
        column_type=postgres_types.JSON,
    ),
    DataType(name='jsonb', type_hint=dict, python_type=dict, column_type=postgres_types.JSONB),
    DataType(name='numeric', type_hint=Decimal, python_type=Decimal, column_type=sa_types.Numeric),
    DataType(name='varchar', type_hint=str, python_type=str, column_type=sa_types.String),
    DataType(
        name='enum', postgres_name='varchar', type_hint=Enum, python_type=Enum, column_type=sa_types.Enum
    ),
]

# Initialize default registry and add the above columns to it
column_registry = ColumnTypeRegistry()

for data_type in COLUMN_TYPES:
    column_registry.add(data_type)
