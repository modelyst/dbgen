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

"""Test that fromJSON . toJSON = identity for various DbGen objects."""
import unittest

from hypothesis import given, settings

from dbgen import Generator, Model, PyBlock, Query
from dbgen.core.datatypes import DataType
from dbgen.core.expr.exprstrat import exprstrat
from dbgen.core.load import Load
from dbgen.example.main import make_model


def serialtest(x):
    """Check reversability of toJSON"""
    assert x == x.fromJSON(x.toJSON())


class TestSerialization(unittest.TestCase):
    """Test the reversability of JSON serialization"""

    @given(DataType._strat())
    def test_datatype(self, x):
        serialtest(x)

    # @given(Attr._strat())
    # def test_attr(self, x):
    #     serialtest(x)

    @given(Load._strat())
    def test_load(self, x):
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

    @settings(deadline=None)
    @given(Generator._strat())
    def test_gen(self, x):
        serialtest(x)

    # @given(Entity._strat())
    # def test_obj(self, x):
    #     serialtest(x)

    # @given(Schema._strat())
    # def test_schema(self, x):
    #     serialtest(x)

    @given(Model._strat())
    def test_model(self, x):
        serialtest(x)


def test_example_serialization():
    """Test the builtin example for serialization"""
    model = make_model()
    serialtest(model)


if __name__ == "__main__":
    unittest.main()
