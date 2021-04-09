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

"""Test the hypothesis strategies for each dbgen object."""
import pytest
from hypothesis import given

from dbgen.core.schema import Attr, Obj, UserRel
from dbgen.core.schemaclass import Schema

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
