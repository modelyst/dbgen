from typing import Type, Any

from hypothesis._strategies import (
    one_of,
    from_type,
    text,
    builds,
    just,  # type: ignore
    booleans,
    dates,
    dictionaries,
    Search_Strategy,
    integers,
)

from hypothesis import given, infer, assume  # type: ignore

from inspect import getfullargspec

from dbgen import (
    Varchar,
    SQLType,
    Attr,
    Arg,
    Const,
    Expr,
    PyBlock,
    Query,
    Gen,
    Obj,
    Model,
)
from dbgen.core.action import Action
from dbgen.core.datatypes import DataType
from dbgen.core.schema import AttrTup
from dbgen.core.schemaclass import Schema
from dbgen.core.expr.exprstrat import exprstrat

"""
Test that fromJSON . toJSON = identity for various DbGen objects.
"""


def serialtest(x):
    return x == x.fromJSON(x.toJSON())


class TestSerialization:
    @given(Varchar._strat())
    def test_varchar(self, x):
        serialtest(x)

    @given(SQLType._strat())
    def test_sqltype(self, x):
        serialtest(x)

    @given(DataType._strat())
    def test_datatype(self, x):
        print(vars(x), x.toJSON())
        serialtest(x)

    @given(Attr._strat())
    def test_attr(self, x):
        serialtest(x)

    @given(Action._strat())
    def test_act(self, x):
        serialtest(x)

    @given(exprstrat())
    def test_expr(self, x):
        serialtest(x)

    @given(PyBlock._strat())
    def test_pyblock(self, x):
        serialtest(x)

    @given(Query._strat())
    def test_query(self, x):
        serialtest(x)

    @given(Gen._strat())
    def test_gen(self, x):
        serialtest(x)

    @given(Obj._strat())
    def test_obj(self, x):
        serialtest(x)

    @given(Schema._strat())
    def test_schema(self, x):
        serialtest(x)

    @given(Model._strat())
    def test_model(self, x):
        serialtest(x)
