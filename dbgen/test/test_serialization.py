"""Test that fromJSON . toJSON = identity for various DbGen objects."""
from hypothesis import given, settings
import unittest
from dbgen import (
    Varchar,
    SQLType,
    Attr,
    PyBlock,
    Query,
    Generator,
    Obj,
    Model,
)
from dbgen.core.load import Load
from dbgen.core.datatypes import DataType
from dbgen.core.schemaclass import Schema
from dbgen.core.expr.exprstrat import exprstrat


def serialtest(x):
    """Check reversability of toJSON"""
    return x == x.fromJSON(x.toJSON())


# class TestSerialization(unittest.TestCase):
#     """Test the reversability of JSON serialization"""

#     @given(Varchar._strat())
#     def test_varchar(self, x):
#         serialtest(x)

#     @given(SQLType._strat())
#     def test_sqltype(self, x):
#         serialtest(x)

#     @given(DataType._strat())
#     def test_datatype(self, x):
#         serialtest(x)

#     @given(Attr._strat())
#     def test_attr(self, x):
#         serialtest(x)

#     @given(Load._strat())
#     def test_load(self, x):
#         serialtest(x)

#     @given(exprstrat())
#     def test_expr(self, x):
#         serialtest(x)

#     @given(PyBlock._strat())
#     def test_pyblock(self, x):
#         serialtest(x)

#     @given(Query._strat())
#     def test_query(self, x):
#         serialtest(x)

#     @settings(deadline=None)
#     @given(Generator._strat())
#     def test_gen(self, x):
#         serialtest(x)

#     @given(Obj._strat())
#     def test_obj(self, x):
#         serialtest(x)

#     @given(Schema._strat())
#     def test_schema(self, x):
#         serialtest(x)

#     @given(Model._strat())
#     def test_model(self, x):
#         serialtest(x)

#     def test_schema_strat(self):
#         schema = Schema._strat().example()
#         assert isinstance(schema, Schema)


# if __name__ == "__main__":
#     unittest.main()
