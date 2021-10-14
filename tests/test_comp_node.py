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

import pytest
from pydantic import ValidationError

from dbgen.core.args import Const
from dbgen.core.computational_node import ComputationalNode

list_of_bad_kwargs = [{"inputs": Const(val="test")}, {"outputs": ["a", "a"]}]


@pytest.mark.parametrize("bad_kwargs", list_of_bad_kwargs)
def test_comp_node_validation(bad_kwargs):
    with pytest.raises(ValidationError):
        ComputationalNode(**bad_kwargs)
