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

"""Tests related to the validation step of a load node."""
import pytest
from pydantic import ValidationError

from dbgen import Entity


class ValidateLoad(Entity, table=True):
    int_field: int
    str_field: str
    opt_field: float = 0.0


load_entity = ValidateLoad._get_load_entity()


def test_simple_type_validation():
    """Simple strict type validation."""
    good_data = {'int_field': 1, 'str_field': '1'}
    assert good_data == load_entity._strict_validate(good_data)
    for bad_data in ({'str_field': 1}, {'int_field': '1'}):
        # Check a coercible data passes _validate but not _strict_validate
        assert good_data == load_entity._validate({**good_data, **bad_data})
        with pytest.raises(ValidationError):
            load_entity._strict_validate({**good_data, **bad_data})


def test_uncoercible_data():
    """Assert validation error is raised when uncoercible data is provided."""
    unocoercible = {'int_field': 'asdf', 'str_field': 'foo'}
    with pytest.raises(ValidationError):
        load_entity._strict_validate(unocoercible)
    with pytest.raises(ValidationError):
        load_entity._validate(unocoercible)


def test_required_data():
    """Test that missing data does not raise any errors."""
    missing_data = {'int_field': 1}
    load_entity._strict_validate(missing_data, insert=False)
    load_entity._validate(missing_data, insert=False)

    with pytest.raises(ValidationError):
        load_entity._strict_validate(missing_data, insert=True)
    with pytest.raises(ValidationError):
        load_entity._validate(missing_data, insert=True)
