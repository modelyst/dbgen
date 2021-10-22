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

from pydantic import validator
from pydantic.fields import PrivateAttr

from dbgen.core.func import Func
from dbgen.core.node.computational_node import ComputationalNode

extractor_type = GenType[Dict[str, Mapping[str, Any]], None, None]


def base_extractor():
    yield {}


base_extractor_func = Func.from_callable(base_extractor)


class Extract(ComputationalNode):
    """
    Base Class for all extraction steps.

    Must implement the extract method which returns a list of rows to map the transforms over.

    Must also be subscriptable to connect to the transforms when coding a model.
    """

    extractor: Func = base_extractor_func
    _extractor: GenType[Dict[str, Any], None, None] = PrivateAttr(None)

    @validator('extractor', pre=True)
    def convert_callable_to_func(cls, extractor):
        if callable(extractor):
            extractor = Func.from_callable(extractor)
        return extractor

    def run(self, _: Dict[str, Mapping[str, Any]]) -> Dict[str, Any]:
        try:
            output = next(self._extractor)
        except StopIteration:
            return None
        return dict(output)

    def set_extractor(self, *, connection=None, yield_per=None, **kwargs):

        if self.extractor is not None:
            self._extractor = self.extractor()

    def get_row_count(self, *, connection=None):
        return None
