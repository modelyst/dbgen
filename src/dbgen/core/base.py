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

import logging
from datetime import date, datetime
from pathlib import Path, PosixPath
from typing import Any, Callable, ClassVar, Dict, Tuple, TypeVar, Union
from uuid import UUID

from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo
from pydantic.main import ModelMetaclass
from pydasher import HashMixIn

_T = TypeVar("_T")


def __dataclass_transform__(
    *,
    eq_default: bool = True,
    order_default: bool = False,
    kw_only_default: bool = False,
    field_descriptors: Tuple[Union[type, Callable[..., Any]], ...] = (()),
) -> Callable[[_T], _T]:
    return lambda a: a


@__dataclass_transform__(kw_only_default=True, field_descriptors=(Field, FieldInfo))
class BaseMeta(ModelMetaclass):
    pass


encoders = {
    UUID: lambda x: str(x),
    datetime: lambda x: x.isoformat(),
    date: lambda x: x.isoformat(),
    Path: lambda x: str(x),
    PosixPath: lambda x: str(x),
}


class Base(HashMixIn, BaseModel, metaclass=BaseMeta):
    """Common methods shared by many DbGen objects."""

    _logger: ClassVar[logging.Logger]
    _logger_name: ClassVar[
        Union[Callable[["Base", Dict[str, Any]], str], str]
    ] = lambda cls, _: cls.canonical_name()

    def __new__(cls, *_args, **kwargs):
        # Call  cls._logger_name function with class and kwargs as arguments
        # to allow dynamic logger names (which is great for table by table loggers)
        logger_name = cls._logger_name(cls, kwargs) if callable(cls._logger_name) else cls._logger_name
        cls._logger = logging.getLogger(logger_name)
        return super().__new__(cls)

    class Config:
        """Pydantic Config"""

        json_encoders = encoders

    @classmethod
    def canonical_name(cls) -> str:
        return cls.__module__ + "." + cls.__qualname__
