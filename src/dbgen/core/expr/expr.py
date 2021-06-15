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

# External Modules
from abc import ABCMeta, abstractmethod
from functools import reduce
from operator import add
from typing import TYPE_CHECKING, Any
from typing import Callable as C
from typing import List as L
from typing import Tuple as T
from typing import Union as U

from hypothesis.strategies import SearchStrategy, builds, just, lists, one_of

# Internal Modules
from dbgen.core.expr.sqltypes import Boolean, Decimal, Int, SQLType, Text, Varchar
from dbgen.utils.lists import concat_map
from dbgen.utils.misc import Base, anystrat

if TYPE_CHECKING:
    from dbgen.core.expr.pathattr import PathAttr

    PathAttr


"""
Python-sql interface.
"""

###############################################################################
Fn = C[[Any], str]  # type shortcut


class Expr(Base, metaclass=ABCMeta):

    # Constants
    # ----------
    @property
    def agg(self) -> bool:
        return False  # by default, we assume not an Aggregation

    # Abstract methods
    # -----------------
    @abstractmethod
    def fields(self) -> list:
        """
        List of immediate substructures of the expression (not recursive)
        """
        raise NotImplementedError

    @abstractmethod
    def show(self, f: Fn) -> str:
        """
        Apply function recursively to fields
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _strat(cls) -> SearchStrategy:
        abc = Literal("abc")
        exprs: L[Expr] = [
            One,
            LT(One, One),
            GT(One, Zero),
            LEN(abc),
            EQ(One, One),
            LEFT(abc, One),
            RIGHT(abc, One),
        ]
        return one_of(*[just(x) for x in exprs])

    # --------------------------#
    # Representing expressions #
    # --------------------------#
    def __str__(self) -> str:
        """
        Default string representation: all fields run through str()
        """
        return self.show(lambda x: str(x))

    def __repr__(self) -> str:
        return f"Expr<{str(self)}>"

    def __hash__(self) -> int:
        return hash(str(self))

    # --------------------#
    # Overloaded methods #
    # --------------------#
    def __abs__(self) -> "ABS":
        return ABS(self)

    def __add__(self, other: "Expr") -> "PLUS":
        return PLUS(self, other)

    def __mul__(self, other: "Expr") -> "MUL":
        return MUL(self, other)

    def __pow__(self, other: "Expr") -> "POW":
        return POW(self, other)

    def __sub__(self, other: "Expr") -> "MINUS":
        return MINUS(self, other)

    def __or__(self, other: Any) -> "Expr":
        raise NotImplementedError

    del __or__  # tricking the type checker to use |Infix|

    def __truediv__(self, other: "Expr") -> "DIV":
        return DIV(self, other)

    # -----------------#
    # Private Methods #
    # -----------------#

    def _all(self) -> list:
        """
        List of all fundamental things that are involved in the expression
        Recursively expand substructures and flatten result
        """
        return concat_map(self.get_all, self.fields())

    # Static methods #

    @staticmethod
    def get_all(x: Any) -> list:
        """If it has any recursive structure to unpack, unpack it"""
        return x._all() if hasattr(x, "_all") else [x]


################################################################################

##############
# Subclasses #
##############


class Unary(Expr):
    """
    Expression that depends on just one individual thing
    """

    # Input-indepedent parameters
    # ----------------------------
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    # Implement Expr abstract methods
    # --------------------------------
    def fields(self) -> list:
        return [self.x]

    def show(self, f: Fn) -> str:
        x = f(self.x)
        return f"{self.name}({x})"

    # Class-specific init
    # -------------------
    def __init__(self, x: Expr) -> None:
        assert isinstance(x, Expr), (x, type(x))
        self.x = x

    @classmethod
    def _strat(cls, strat: SearchStrategy = None) -> SearchStrategy:
        from dbgen.core.expr.exprstrat import exprstrat

        x = exprstrat if strat is None else strat
        assert isinstance(x, SearchStrategy)
        return builds(cls, x=x)


class Binary(Expr):
    """
    Expression that depends on two individual things
    """

    # Input-indepedent parameters
    # ----------------------------
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    infix = True

    # Implement Expr abstract methods
    # --------------------------------
    def fields(self) -> list:
        return [self.x, self.y]

    def show(self, f: Fn) -> str:
        x, y = f(self.x), f(self.y)
        if self.infix:
            return f"({x} {self.name} {y})"
        else:
            return f"{self.name}({x},{y})"

    # Class-specific init
    # -------------------
    def __init__(self, x: Expr, y: Expr) -> None:
        assert all([isinstance(a, Expr) for a in [x, y]]), [
            x,
            type(x),
            y,
            type(y),
        ]
        self.x, self.y = x, y

    @classmethod
    def _strat(cls, strat: SearchStrategy = None) -> SearchStrategy:
        from dbgen.core.expr.exprstrat import exprstrat

        x = exprstrat() if strat is None else strat
        return builds(cls, x=x, y=x)


class Ternary(Expr):
    """
    Expression that depends on three individual things
    """

    # Input-indepedent parameters
    # ----------------------------
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    # Implement Expr abstract methods
    # --------------------------------
    def fields(self) -> list:
        return [self.x, self.y, self.z]

    def show(self, f: Fn) -> str:
        x, y, z = f(self.x), f(self.y), f(self.z)
        return f"{self.name}({x},{y},{z})"

    # Class-specific init
    # -------------------
    def __init__(self, x: Expr, y: Expr, z: Expr) -> None:
        assert all([isinstance(a, Expr) for a in [x, y, z]])
        self.x, self.y, self.z = x, y, z

    @classmethod
    def _strat(cls, strat: SearchStrategy = None) -> SearchStrategy:
        from dbgen.core.expr.exprstrat import exprstrat

        x = exprstrat() if strat is None else strat
        return builds(cls, x=x, y=x, z=x)


class Nary(Expr):
    """
    SQL Functions that take multiple arguments, initialized by user with
    multiple inputs (i.e. not a single list input)
    """

    # Input-indepedent parameters
    # ----------------------------
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    def delim(self) -> str:
        return ","  # default delimiter

    def fields(self) -> list:
        return self.xs

    def __init__(self, xs: L[Expr]) -> None:
        self.xs = xs
        assert all([isinstance(x, Expr) for x in xs])

    def show(self, f: Fn) -> str:
        xs = map(f, self.xs)
        d = f" {self.delim} "
        return f"{self.name}({d.join(xs)})"

    @classmethod
    def _strat(cls, strat: SearchStrategy = None) -> SearchStrategy:
        from dbgen.core.expr.exprstrat import exprstrat

        x = exprstrat() if strat is None else strat
        return builds(cls, xs=lists(x, min_size=1, max_size=2))


class Named(Expr):
    """
    Inherit from this to allow any arbitrary class, e.g. XYZ(object),
     to automatically deduce that its 'name' property should be 'XYZ'
    """

    @property
    def name(self) -> str:
        return type(self).__name__


class Agg(Expr):
    """
    This class is meant to be inherited by any SQL function we want to flag
    as an aggregation.

    We can optionally specify what objects we want to aggregate over, otherwise
    the intent will be guessed
    """

    @property
    def agg(self) -> bool:
        return True

    def __init__(self, x: Expr, objs: list = None) -> None:
        assert issubclass(type(x), Expr)
        self.x = x
        self.objs = objs or []


################################################################################
# Specific Expr classes for user interface
###########################################

# Ones that work out of the box
# ------------------------------
class ABS(Named, Unary):
    pass


class SQRT(Named, Unary):
    pass


class MAX(Named, Agg, Unary):
    pass


class SUM(Named, Agg, Unary):
    pass


class MIN(Named, Agg, Unary):
    pass


class AVG(Named, Agg, Unary):
    pass


class COUNT(Agg, Named, Unary):
    pass


class CONCAT(Named, Nary):
    pass


class BINARY(Named, Unary):
    pass  # haha


class REGEXP(Named, Binary):
    pass


class REPLACE(Named, Ternary):
    pass


class COALESCE(Named, Nary):
    pass


class LIKE(Named, Binary):
    pass


# Ones that need a field defined
# -------------------------------
class Tup(Nary):
    name = ""


class LEN(Unary):
    name = "CHAR_LENGTH"


class MUL(Binary):
    name = "*"


class DIV(Binary):
    name = "/"


class PLUS(Binary):
    name = "+"


class MINUS(Binary):
    name = "-"


class POW(Named, Binary):
    infix = False


class LEFT(Named, Binary):
    infix = False


class RIGHT(Named, Binary):
    infix = False


class JSON_EXTRACT(Named, Binary):
    infix = False


class EQ(Binary):
    name = "="


class NE(Binary):
    name = "!="


class LT(Binary):
    name = "<"


class GT(Binary):
    name = ">"


class LE(Binary):
    name = "<="


class GE(Binary):
    name = ">="


class OR(Nary):
    """ Can be used as a binary operator (|OR|) or as a function OR(a,b,...)"""

    name = ""
    delim = "OR"


class AND(Nary):
    name = ""
    delim = "\n\tAND"


class And(Nary):
    name = ""
    delim = "\n\tAND"


class NOT(Named, Unary):
    wrap = False


class NULL(Named, Unary):
    def show(self, f: Fn) -> str:
        return f"{f(self.x)} is NULL"


class ARRAY(Named, Nary):
    def show(self, f: Fn) -> str:
        xs = map(f, self.xs)
        d = f" {self.delim} "
        return f"{self.name}[{d.join(xs)}]"


# Ones that need to be implemented from scratch
# ----------------------------------------------


class Literal(Expr):
    def __init__(self, x: Any) -> None:
        self.x = x

    def fields(self) -> L[Expr]:
        return []

    def show(self, f: Fn) -> str:

        if isinstance(self.x, str):
            return "('%s')" % f(self.x).replace("'", "\\'").replace("%", "%%")
        elif self.x is None:
            return "(NULL)"
        else:
            x = f(self.x)
            return f"({x})"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls, x=anystrat)


class IN(Named):
    def __init__(self, x: Expr, xs: U[L[Expr], L[Literal]]) -> None:
        self.x = x
        self.xs = xs

    def fields(self) -> L[Expr]:
        return [self.x] + self.xs  # type: ignore

    def show(self, f: Fn) -> str:
        xs = map(f, self.xs)
        return f"{f(self.x)} IN ({','.join(xs)})"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


##########
class CASE(Expr):
    def __init__(self, cases: L[T[Expr, Expr]], else_: Expr) -> None:
        self.cases = cases
        self.else_ = else_

    def fields(self) -> L[Expr]:
        k, v = map(lambda x: list(x), zip(*self.cases))
        return k + v + [self.else_]

    def show(self, f: Fn) -> str:
        body = " ".join(["WHEN ({}) THEN ({})".format(f(k), f(v)) for k, v in self.cases])
        end = f" ELSE ({f(self.else_)}) END"
        return "CASE  " + body + end

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class IF_ELSE(Expr):
    def __init__(self, cond: Expr, if_: Expr, else_: Expr) -> None:
        self.cond = cond
        self.if_ = if_
        self.else_ = else_

    def fields(self) -> L[Expr]:
        return [self.cond, self.if_, self.else_]

    def show(self, f: Fn) -> str:
        c, i, e = map(f, self.fields())
        return f"CASE WHEN ({c}) THEN ({i}) ELSE ({e}) END"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class CONVERT(Expr):
    def __init__(self, expr: Expr, dtype: SQLType) -> None:
        self.expr = expr
        self.dtype = dtype

        err = "Are you SURE that Postgres can convert to this dtype? %s\n Also did you make sure to use an instance and not the class itself?"
        assert isinstance(dtype, (Decimal, Varchar, Text, Int, Boolean)), err % dtype

    def fields(self) -> L[Expr]:
        return [self.expr]

    def show(self, f: Fn) -> str:
        e = f(self.expr)
        return f"CAST({e} AS {self.dtype})"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class SUBSELECT(Expr):
    """Hacky way of getting in subselect ... will not automatically detect
    dependencies."""

    def __init__(self, expr: Expr, tab: str, where: str = "1") -> None:
        self.expr = expr
        self.tab = tab
        self.where = where

    def fields(self) -> L[Expr]:
        return [self.expr]

    def show(self, f: Fn) -> str:
        e = f(self.expr)
        return f"(SELECT {e} FROM {self.tab} WHERE {self.where} )"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class GROUP_CONCAT(Agg):
    def __init__(self, expr: Expr, delim: str = None, order: Expr = None) -> None:
        self.expr = expr
        self.delim = delim or ","
        self.order = order

    def fields(self) -> L[Expr]:
        return [self.expr] + ([self.order] if self.order is not None else [])

    @property
    def name(self) -> str:
        return "string_agg"

    def show(self, f: Fn) -> str:
        ord = "ORDER BY " + f(self.order) if self.order is not None else ""

        return f"string_agg({f(self.expr)} :: TEXT,'{self.delim}' {ord})"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class ARRAY_AGG(Agg):
    def __init__(self, expr: Expr, order: Expr = None) -> None:
        self.expr = expr
        self.order = order

    def fields(self) -> L[Expr]:
        return [self.expr] + ([self.order] if self.order is not None else [])

    @property
    def name(self) -> str:
        return "array_agg"

    def show(self, f: Fn) -> str:
        ord = "ORDER BY " + f(self.order) if self.order is not None else ""

        return f"array_agg({f(self.expr)} {ord})"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class STD(Agg, Unary):
    name = "stddev_pop"


class POSITION(Named, Binary):
    def show(self, f: Fn) -> str:
        return f"POSITION({f(self.x)} in {f(self.y)})"


##############################################################################
##############################################################################
class PK(Expr):
    """Special Expr type for providing PK + UID info"""

    def __init__(self, pk: "PathAttr") -> None:
        self.pk = pk

    @property
    def name(self) -> str:
        return "PK"

    def show(self, f: Fn) -> str:
        return f(self.pk)

    def fields(self) -> list:
        return [self.pk]

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


############################
# Specific Exprs and Funcs #
############################

Zero = Literal(0)
One = Literal(1)
true = Literal("true")
false = Literal("false")


def Sum(iterable: L[Expr]) -> Expr:
    """The builtin 'sum' function doesn't play with non-integers"""
    return reduce(add, iterable, Zero)


def R2(e1: Expr, e2: Expr) -> Expr:
    """
    Pearson correlation coefficient for two independent vars
    "Goodness of fit" for a model y=x, valued between 0 and 1
    """
    return (AVG(e1 * e2) - (AVG(e1) * AVG(e2))) / (STD(e1) * STD(e2))


def toDecimal(e: Expr) -> Expr:
    return CONVERT(e, Decimal())
