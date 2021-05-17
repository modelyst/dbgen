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

from abc import ABCMeta, abstractmethod
from datetime import datetime
from random import choice
from re import split
from string import ascii_lowercase, ascii_uppercase, digits

# External Modules
from typing import Any, Optional

from dbgen.utils.exceptions import DBgenTypeError
from dbgen.utils.misc import Base

"""
Representations of SQL Data Types
"""
################################################################################
chars = ascii_lowercase + ascii_uppercase + digits


class SQLType(Base, metaclass=ABCMeta):
    """
    SQL datatypes
    """

    data = {}  # type: dict

    @abstractmethod
    def __str__(self) -> str:
        """String representation to be used in raw SQL expression"""
        raise NotImplementedError

    @abstractmethod
    def cast(self, val):
        """Cast a value as SQLType"""
        raise NotImplementedError

    def __init__(self) -> None:
        pass

    @staticmethod
    def from_str(s: str) -> "SQLType":
        """
        Ad hoc string parsing
        """
        if "VARCHAR" in s:
            mem = split(r"\(|\)", s)[1]
            return Varchar(int(mem))
        elif "DECIMAL" in s:
            prec, scale = split(r"\(|\)|,", s)[1:3]
            return Decimal(int(prec), int(scale))
        elif "INT" in s:
            if "TINY" in s:
                kind = "small"
            elif "BIG" in s:
                kind = "big"
            else:
                kind = "medium"
            signed = "UNSIGNED" not in s
            return Int(kind, signed)
        elif "TEXT" in s:
            if "TINY" in s:
                kind = "tiny"
            elif "MED" in s:
                kind = "medium"
            elif "LONG" in s:
                kind = "long"
            else:
                kind = ""
            return Text(kind)
        else:
            raise NotImplementedError("New SQLtype to parse? " + s)


class Varchar(SQLType):
    def __init__(self, mem: int = 255) -> None:
        self.mem = mem

    def __str__(self) -> str:
        return "VARCHAR(%d)" % self.mem

    def cast(self, val) -> Optional[str]:
        if val is None:
            return val
        return str(val)


class Decimal(SQLType):
    def __init__(self, prec: int = 15, scale: int = 6) -> None:
        self.prec = prec
        self.scale = scale

    def __str__(self) -> str:
        return "DECIMAL(%d,%d)" % (self.prec, self.scale)

    def cast(self, val) -> Optional[float]:
        if val is None:
            return val
        return float(val)


class Boolean(SQLType):
    def __init__(self) -> None:
        pass

    def __str__(self) -> str:
        return "Boolean"

    def rand(self) -> Any:
        return choice(["true", "false"])

    def cast(self, val) -> Optional[bool]:
        if val is None:
            return val
        elif not isinstance(val, bool):
            raise DBgenTypeError(f"Val is not a boolean! {val}")
        return bool(val)


class Int(SQLType):
    def __init__(
        self,
        kind: str = "medium",
        signed: bool = True,
    ) -> None:
        kinds = ["small", "medium", "big"]
        assert kind in kinds, f"Invalid Int type: {kind} not found in {kinds}"
        self.kind = kind
        self.signed = signed

    def __str__(self) -> str:
        options = ["small", "medium", "big"]
        if self.kind == "small":
            core = "SMALLINT"
        elif self.kind == "medium":
            core = "INTEGER"
        elif self.kind == "big":
            core = "BIGINT"
        else:
            err = 'unknown Int kind "%s" not in options %s '
            raise ValueError(err % (self.kind, options))
        return core + ("" if self.signed else " UNSIGNED")

    def cast(self, val) -> Optional[int]:
        if val is None:
            return val
        elif not isinstance(val, (int, float, str)) or isinstance(val, bool):
            raise DBgenTypeError(
                f"Val cannot be safely cast to int type: {val} {type(val)}\nSafe types are int, str, float."
            )
        return int(val)


class Text(SQLType):
    def __init__(self, kind: str = "") -> None:
        self.kind = kind

    def __str__(self) -> str:
        return "TEXT"

    def cast(self, val) -> Optional[str]:
        return str(val)


class Date(SQLType):
    def __str__(self) -> str:
        return "DATE"

    def cast(self, val) -> Optional[datetime]:
        if val is None:
            return val
        elif not isinstance(val, datetime):
            raise DBgenTypeError(f"Value cannot be safely cast to {str(self)}: {val}")
        return val


class Timestamp(SQLType):
    def __str__(self) -> str:
        return "TIMESTAMP"

    def cast(self, val) -> Optional[datetime]:
        if val is None:
            return val
        elif not isinstance(val, datetime):
            raise DBgenTypeError(f"Value cannot be safely cast to {str(self)}: {val}")
        return val


class Double(SQLType):
    def __str__(self) -> str:
        return "DOUBLE"

    def cast(self, val) -> Optional[float]:
        if val is None:
            return val
        elif not isinstance(val, float):
            raise DBgenTypeError(f"Value cannot be safely cast to {str(self)}: {val}")
        return val


class JSON(SQLType):
    def __str__(self) -> str:
        return "JSON"

    def cast(self, val) -> Optional[str]:
        if val is None:
            return val
        elif not isinstance(val, str):
            raise DBgenTypeError(f"Value cannot be safely cast to {str(self)}: {val}")
        return val


class JSONB(SQLType):
    def __str__(self) -> str:
        return "JSONB"

    def cast(self, val) -> Optional[str]:
        if val is None:
            return val
        elif not isinstance(val, str):
            raise DBgenTypeError(f"Value cannot be safely cast to {str(self)}: {val}")
        return val
