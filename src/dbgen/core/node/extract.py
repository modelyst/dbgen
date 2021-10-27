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
from typing import Mapping, Optional

from pydantic.fields import PrivateAttr

from dbgen.core.node.computational_node import ComputationalNode

extractor_type = GenType[Dict[str, Mapping[str, Any]], None, None]


class Extract(ComputationalNode):
    """
    Base Class for all extraction steps.

    Must implement the extract method which returns a list of rows to map the transforms over.

    Must also be subscriptable to connect to the transforms when coding a model.
    """

    _extractor: GenType[Dict[str, Any], None, None] = PrivateAttr(None)

    # Overwrite these when writing custom extractor
    def setup(self, **_) -> Optional[GenType[Dict[str, Any], None, None]]:
        pass

    def extract(self):
        yield {}

    def teardown(self, **_):
        pass

    def length(self, **_):
        return None

    # Internal Do not Overwrite
    def run(self, _: Dict[str, Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
        try:
            output = next(self._extractor)
        except StopIteration:
            return None
        return dict(output)

    def set_extractor(self, *, connection=None, yield_per=None, **kwargs):
        extractor = self.setup(connection=connection, yield_per=yield_per, **kwargs)
        # Check if setup assigned the extractor already
        if extractor is not None:
            self._extractor = extractor
        elif getattr(self, 'extract', None) and callable(self.extract):
            self._extractor = self.extract()
        else:
            raise NotImplementedError(f"No way of getting the extractor from this extract")

    def __str__(self):
        return f"Extract<{self.outputs}>"
