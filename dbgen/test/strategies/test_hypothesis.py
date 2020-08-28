"""Test the hypothesis strategies for each dbgen object."""
import pytest
from hypothesis import given

from ...core.schema import Attr, Obj, UserRel
from ...core.schemaclass import Schema
from . import get_strategy
from .config import settings  # noqa: F401

dbgen_objects_to_test = (Attr, Obj, UserRel, Schema)


@pytest.mark.filterwarnings("ignore")
def test_get_strategy():
    for dbgen_object in dbgen_objects_to_test:
        strat = get_strategy(dbgen_object)
        example = strat.example()
        assert isinstance(example, dbgen_object)


class TestClass:
    """Test the hypothesis strategies for each dbgen object"""

    @given(get_strategy(Attr))
    def test_attr_strat(self, attr):
        assert isinstance(attr, Attr)

    @given(get_strategy(UserRel))
    def test_user_rel_strat(self, user_rel):
        assert isinstance(user_rel, UserRel)

    @given(get_strategy(Obj))
    def test_obj_strat(self, obj):
        assert isinstance(obj, Obj)

    @given(get_strategy(Schema))
    def test_schema_strat(self, schema: Schema):
        assert isinstance(schema, Schema)
