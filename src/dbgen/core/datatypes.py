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
from ast import literal_eval
from inspect import _empty  # type: ignore

# External Modules
from typing import List as L
from typing import Type
from typing import TypeVar as TV

from hypothesis.strategies import SearchStrategy, builds, just, lists, one_of

# Internal modules
from dbgen.utils.misc import Base

"""
Defines the abstract class DataType and all classes that implement it
"""
################################################################################
class DataType(Base, metaclass=ABCMeta):
    """
    Internal representation of Python datatypes

    This is needed because the output datatypes from python's 'inspect' module
    are not serializable. To convert such an output into an instance of this
    class, use the class method DataType.get_datatype()

    This class is meant to be extended as new cases appear
    """

    def __init__(self) -> None:
        super().__init__()

    def __len__(self) -> int:
        """Default length, ONLY Tuple has a non-one length."""
        return 1

    @classmethod
    def _strat(cls) -> SearchStrategy:
        dts = [
            AnyType(),
            NoneType(),
            BaseType("x"),
            TypeVar("x"),
            Callable([AnyType()], NoneType()),
            Union([AnyType(), BaseType("x")]),
            Tuple([AnyType(), BaseType("x")]),
            List(NoneType()),
            Dict(BaseType("x"), BaseType("y")),
        ]  # type: L[DataType]
        return one_of(*[just(x) for x in dts])

    @abstractmethod
    def __str__(self) -> str:
        """Need to provide a str representation"""
        raise NotImplementedError

    @classmethod
    def get_datatype(cls, t: Type) -> "DataType":
        """
        Convert a Python Type object into a simple representation

        This is very ad hoc and hacky
        """
        if isinstance(t, str):
            try:
                t = literal_eval(t)  # try to evaluate, likely will fail
            except ValueError:
                pass

        strt = str(t)

        # Python builtins
        if t == float:
            return BaseType("Float")
        elif t == int:
            return BaseType("Int")
        elif t == str:
            return BaseType("Str")
        elif t == bool:
            return BaseType("Bool")
        elif isinstance(t, type(None)):
            return NoneType()

        # Special cases from chemistry libraries
        elif "Atoms" in strt:
            return BaseType("Atoms")
        elif strt == "Structure":
            return BaseType("Structure")
        elif strt in ["BULK", "<class 'bulk_enumerator.bulk.BULK'>"]:
            return BaseType("BULK")
        elif "Graph" in strt:
            return BaseType("MultiGraph")
        elif "Structure" in strt:
            return BaseType("Structure")

        # Typing higher order types
        elif str(t.__class__) == str(Union) or strt[:12] == "typing.Union":
            return Union([cls.get_datatype(x) for x in t.__args__])

        elif "Tuple" in strt:
            return Tuple([cls.get_datatype(x) for x in t.__args__])

        elif "Callable" in strt:
            from_args = [cls.get_datatype(x) for x in t.__args__[:-1]]
            to_arg = cls.get_datatype(t.__args__[-1])
            return Callable(from_args, to_arg)

        elif isinstance(t, TV):  # type: ignore
            return TypeVar(t.__name__)

        elif strt[:11] == "typing.List":
            return List(cls.get_datatype(t.__args__[0]))

        elif strt[:11] == "typing.Dict":
            a1, a2 = [cls.get_datatype(x) for x in t.__args__]
            return Dict(a1, a2)

        elif strt == "typing.Any" or t == _empty:  # type: ignore
            return AnyType()

        else:
            raise NotImplementedError(f"NEW DATATYPE FOUND {strt}")


################################################################################


class AnyType(DataType):
    def __str__(self) -> str:
        return "Any"
        super().__init__()

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


################################################################################


class NoneType(DataType):
    def __str__(self) -> str:
        return "None"
        super().__init__()

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


################################################################################


class BaseType(DataType):
    def __init__(self, unBase: str) -> None:
        self.unBase = unBase
        super().__init__()

    def __str__(self) -> str:
        return f'"{self.unBase}"'

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


################################################################################


class TypeVar(DataType):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__()

    def __str__(self) -> str:
        return f'"{self.name}"'

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


################################################################################


class Callable(DataType):
    def __init__(self, c_args: L[DataType], out: DataType) -> None:
        self.c_args = c_args
        self.out = out
        super().__init__()

    def __str__(self) -> str:
        ar = " -> "
        return ar.join(map(str, self.c_args)) + ar + str(self.out)

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(
            cls,
            out=DataType._strat(),
            c_args=lists(DataType._strat(), min_size=1, max_size=2),
        )


################################################################################


class Union(DataType):
    def __init__(self, args: L[DataType]) -> None:
        self.args = args
        super().__init__()

    def __str__(self) -> str:
        return "{%s}" % (",".join(sorted(map(str, self.args))))

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls, args=lists(DataType._strat(), min_size=1, max_size=2))


################################################################################


class Tuple(DataType):
    def __init__(self, args: L[DataType]) -> None:
        self.args = args
        super().__init__()

    def __str__(self) -> str:
        return f"({','.join(map(str, self.args))})"

    def __len__(self) -> int:
        return len(self.args)

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls, args=lists(DataType._strat(), min_size=1, max_size=2))


################################################################################


class List(DataType):
    def __init__(self, content: DataType) -> None:
        self.content = content
        super().__init__()

    def __str__(self) -> str:
        return f"[{str(self.content)}]"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


################################################################################


class Dict(DataType):
    def __init__(self, key: DataType, val: DataType) -> None:
        self.key = key
        self.val = val
        super().__init__()

    def __str__(self) -> str:
        return f"{{ {str(self.key)} : {str(self.val)} }}"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)
