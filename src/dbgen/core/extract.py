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

from typing import Any, Dict
from typing import Generator as GenType
from typing import Mapping

from dbgen.core.computational_node import ComputationalNode

extractor_type = GenType[Dict[str, Mapping[str, Any]], None, None]


class Extract(ComputationalNode):
    """
    Base Class for all extraction steps.

    Must implement the extract method which returns a list of rows to map the transforms over.

    Must also be subscriptable to connect to the transforms when coding a model.
    """

    def extract(self, *, connection=None, yield_per=None, **kwargs) -> extractor_type:
        yield {}

    def get_row_count(self, *, connection=None):
        return None
