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
from copy import deepcopy
from importlib import import_module
from inspect import getfullargspec
from json import dumps, loads
from string import ascii_lowercase
from typing import Any
from typing import Dict as D
from typing import List as L
from typing import Type, TypeVar
from typing import Union as U

from hypothesis import infer
from hypothesis.strategies import SearchStrategy, booleans, builds, integers, none, one_of, text

from dbgen.utils.str_utils import hash_

NoneType = type(None)

##############################################################################

T = TypeVar("T")


def identity(x: T) -> T:
    return x


def kwargs(x: Any) -> L[str]:
    return sorted(getfullargspec(type(x))[0][1:])


anystrat = one_of(text(), booleans(), text(), integers(), none())
nonempty = text(min_size=1)
nonempty_limited = text(min_size=1, max_size=3)
letters = text(min_size=1, alphabet=ascii_lowercase)


def build(typ: Type) -> SearchStrategy:
    """Unfortunately, hypothesis cannot automatically ignore default kwargs."""
    args, _, _, _, _, _, annotations = getfullargspec(typ)
    # use default kwarg value if type is Any
    kwargs = {k: infer for k in args[1:] if annotations[k] != Any}
    return builds(typ, **kwargs)


simple = ["int", "str", "float", "NoneType", "bool"]
complex = ["tuple", "list", "set", "dict"]


def to_dict(x: Any, id_only: bool = False) -> U[L, int, float, str, D[str, Any], NoneType]:
    """Create JSON serializable structure for arbitrary Python/DbGen type."""
    module, ptype = type(x).__module__, type(x).__name__
    metadata = dict(_pytype=module + "." + ptype)  # type: D[str, Any]
    if module == "builtins" and ptype in simple:
        return x
    elif module == "builtins" and ptype in complex:
        if ptype == "dict":
            assert all([isinstance(k, str) for k in x.keys()]), x
            return {k: to_dict(v, id_only) for k, v in x.items() if not (k == "_uid" and id_only)}
        elif ptype == "list":
            return [to_dict(xx, id_only) for xx in x]
        elif ptype == "tuple":
            return dict(**metadata, _value=[to_dict(xx, id_only) for xx in x])
        elif ptype == "set":
            return dict(**metadata, _value=[to_dict(xx, id_only) for xx in sorted(x)])
        else:
            raise TypeError(x)
    else:
        assert hasattr(x, "__dict__"), metadata
        data = {
            k: to_dict(v, id_only)
            for k, v in sorted(vars(x).items())
            if (k in kwargs(x)) or (not id_only and k[0] != "_")
        }
        if not id_only:
            hashdict = {
                **metadata,
                **{k: to_dict(v, True) for k, v in sorted(vars(x).items()) if (k in kwargs(x))},
            }
            metadata["_uid"] = hash_(hashdict)
        return {**metadata, **data}


def from_dict(x: Any) -> Any:
    """Create a python/DbGen type from a JSON serializable structure."""
    if isinstance(x, dict):
        ptype = x.get("_pytype", "")
        if "dbgen" in ptype:
            mod, cname = ".".join(ptype.split(".")[:-1]), ptype.split(".")[-1]
            constructor = getattr(import_module(mod), cname)
            return constructor(
                **{k: from_dict(v) for k, v in x.items() if k in getfullargspec(constructor)[0][1:]}
            )
        elif ptype in ("builtins/tuple", "builtins.tuple"):
            return tuple([from_dict(xx) for xx in x["_value"]])  # data-level tuple
        elif ptype in ("builtins/set", "builtins.set"):
            return {from_dict(xx) for xx in x["_value"]}  # data-level tuple
        else:
            assert "_ptype" not in x
            return {k: from_dict(v) for k, v in x.items()}  # data-level dict
    elif isinstance(x, (int, float, list, type(None), str)):
        if isinstance(x, list):
            return [from_dict(xx) for xx in x]
        else:
            return x
    else:
        raise TypeError(x)


class Base(metaclass=ABCMeta):
    """Common methods shared by many DbGen objects."""

    def __init__(self) -> None:
        fields = set(vars(self))
        args = set(kwargs(self))
        missing = args - fields
        assert not missing, f"Need to store args {missing} of {self}"
        assert not any([a[0] == "_" for a in args]), args

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: Any) -> bool:
        """
        Maybe the below should be preferred? Try it out, sometime!
        return type(self) == type(other) and vars(self) == vars(other)
        """
        if type(other) == type(self):
            return hash(self) == hash(other)
        else:
            args = [self, type(self), other, type(other)]
            err = "Equality type error \n{} \n({}) \n\n{} \n({})"
            raise ValueError(err.format(*args))

    def copy(self: T) -> T:
        return deepcopy(self)

    def toJSON(self, id_only=False) -> str:
        return dumps(to_dict(self, id_only), indent=4, sort_keys=True)

    @staticmethod
    def fromJSON(s: str) -> "Base":
        val = from_dict(loads(s))
        assert isinstance(val, Base)
        return val

    def __hash__(self) -> int:
        return int(self.hash)

    @property
    def hash(self) -> str:
        return hash_(to_dict(self, id_only=True))

    @classmethod
    def canonical_name(cls) -> str:
        return cls.__module__ + "." + cls.__qualname__
