"""Module for testing the load object"""
# External Modules
import unittest

# Internal Modules
from dbgen import Model, Obj, Attr, Const, Rel
import dbgen.utils.exceptions as exceptions


class TestLoad(unittest.TestCase):
    """Test the load object"""

    def setUp(self) -> None:
        self.model = Model("test_model")
        parent_obj = Obj("parent", attrs=[Attr("test_id_col", identifying=True)],)
        test_object = Obj(
            "child",
            attrs=[Attr("test_id_col", identifying=True), Attr("test_col")],
            fks=[Rel("parent", identifying=True)],
        )
        self.model.add([parent_obj, test_object])

    def test_simple_load_creation(self) -> None:
        """Creates an example load from an Obj with no expected errors"""
        test_object = self.model.get("child")
        test_object(parent=Const(None), test_id_col=Const(1), test_col=Const(None))

    def test_load_exceptions(self) -> None:
        """Creates an example load from an Obj and tests for the error messaging"""
        test_object = self.model.get("child")
        with self.assertRaises(exceptions.DBgenMissingInfo):
            test_object()
        with self.assertRaises(exceptions.DBgenMissingInfo):
            test_object(test_col=Const(None))
        with self.assertRaises(exceptions.DBgenMissingInfo):
            test_object(test_id_col=Const(1), test_col=Const(None))
        with self.assertRaises(exceptions.DBgenInvalidArgument):
            test_object(parent=Const(None), test_id_col=1, test_col=Const(None))
