"""Test methods for the Base dbgen object."""

from dbgen.utils.misc import Base


def test_canonical_name():
    """Tests the canoncial_name method on the base class"""
    base_obj = Base
    assert base_obj.canonical_name() == "dbgen.utils.misc.Base"
