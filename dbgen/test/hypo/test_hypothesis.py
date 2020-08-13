"""Test that fromJSON . toJSON = identity for various DbGen objects."""
import unittest

from hypothesis import given

from dbgen.core.schema import Attr, Obj, UserRel
from . import STRATEGIES


class DBGenCoreSchema(unittest.TestCase):
    """Test the hypothesis strategies for each dbgen object"""

    @given(STRATEGIES["Attr"])
    def test_attr_strat(self, attr):
        assert isinstance(attr, Attr)

    @given(STRATEGIES["UserRel"])
    def test_user_rel_strat(self, user_rel):
        assert isinstance(user_rel, UserRel)

    @given(STRATEGIES["Obj"])
    def test_obj_strat(self, obj):
        assert isinstance(obj, Obj)
