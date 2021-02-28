"""Tests for the PyBlock Object"""
# External imports
from unittest import TestCase

# Internal Imports
from dbgen.core.funclike import PyBlock, Const
import dbgen.utils.exceptions as dbgen_exc


def transform_method(input_str: str) -> str:
    return input_str + " transformed"


class TestPyBlock(TestCase):
    """Test Cases for the PyBlock object of dbgen."""

    @staticmethod
    def transform_method(input_str: str) -> str:
        return input_str + " transformed"

    def setUp(self):
        self.transform = lambda x: [i + 1 for i in x]

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
        self.assertEqual(namespace[str(hash(pyblock_2))]["out"], "input transformed transformed")
