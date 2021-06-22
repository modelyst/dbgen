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

"""Tests for the PyBlock Object"""
# External imports
from unittest import TestCase

import dbgen.utils.exceptions as dbgen_exc

# Internal Imports
from dbgen.core.funclike import Const, PyBlock


def transform_method(input_str: str) -> str:
    return input_str + " transformed"


def transform_list(x):
    return [i + 1 for i in x]


class TestPyBlock(TestCase):
    """Test Cases for the PyBlock object of dbgen."""

    def setUp(self):
        self.transform = transform_list

    def test_simple_creation(self):
        """test the creation of a simple PyBlock"""
        pyblock = PyBlock(self.transform)
        self.assertTrue(callable(pyblock))

    def test_built_in_function_instatiation(self):
        """test the error generated when using built in functions in PyBlock"""
        # with self.assertRaises(dbgen_exc.DBgenInvalidArgument):
        builtins = (str, min, list, [].append, "".join)
        for builtin in builtins:
            with self.assertRaises(dbgen_exc.DBgenInvalidArgument):
                PyBlock(builtin)

    def test_missing_hash_exc(self):
        """Test for the exception when the args to a PyBlock are at a unknown
        hash"""
        missing_pyblock = PyBlock(lambda x: 1)
        pyblock = PyBlock(self.transform, args=[missing_pyblock["out"]])
        with self.assertRaises(dbgen_exc.DBgenMissingInfo):
            _ = pyblock({})
            print(_)

    def test_simple_pyblock_output_lambda(self):
        """tests a simple case of calling a lambda PyBlock"""
        inputs = {"query": Const([1, 2, 3])}
        pyblock = PyBlock(self.transform, args=[inputs["query"]])
        output = pyblock(inputs)
        self.assertEqual(output, {"out": [2, 3, 4]})

    def test_simple_pyblock_output_static_method(self):
        """tests a simple case of calling a static_method PyBlock"""
        inputs = {"query": Const("input")}
        pyblock = PyBlock(transform_method, args=[inputs["query"]])
        output = pyblock(inputs)
        self.assertEqual(output, {"out": "input transformed"})

    def test_multiple_pyblocks(self):
        namespace = {"query": Const("input")}
        pyblock_1 = PyBlock(transform_method, args=[namespace["query"]])
        pyblock_2 = PyBlock(transform_method, args=[pyblock_1["out"]])
        for pyblock in (pyblock_1, pyblock_2):
            namespace.update({str(hash(pyblock)): pyblock(namespace)})
        self.assertEqual(namespace[str(hash(pyblock_1))]["out"], "input transformed")
        self.assertEqual(
            namespace[str(hash(pyblock_2))]["out"],
            "input transformed transformed",
        )
