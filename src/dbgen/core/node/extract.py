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
from typing import Mapping, Optional, Sequence, TypeVar, Union

from pydantic import PrivateAttr

from dbgen.core.node.computational_node import ComputationalNode

extractor_type = GenType[Dict[str, Mapping[str, Any]], None, None]

T = TypeVar('T')
T1 = TypeVar('T1')


class Extract(ComputationalNode[T]):
    """
    Base Class for all extraction steps.

    Must implement the extract method which returns a list of rows to map the transforms over.

    Must also be subscriptable to connect to the transforms when coding a model.
    """

    _extractor: GenType[Union[Dict[str, Any], T], None, None] = PrivateAttr(None)

    class Config:
        """Pydantic Config"""

        underscore_attrs_are_private = True

    # Overwrite these when writing custom extractor
    def setup(self, **_) -> Optional[GenType[Any, None, None]]:
        pass

    def extract(self: 'Extract[T]') -> GenType[T, None, None]:
        yield {}  # type: ignore

    def teardown(self, **_):
        pass

    def length(self, **_):
        return None

    # Internal Do not Overwrite

    def run(self, _: Dict[str, Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
        try:
            output = next(self._extractor)
            if output is None:
                return {}
            elif isinstance(output, Mapping):
                return dict(output)
            elif isinstance(output, Sequence):
                l1, l2 = len(output), len(self.outputs)
                if l1 != l2:
                    raise ValueError(f"Expected {l2} from extract {self}, but got {l1}")
                return {name: val for name, val in zip(self.outputs, output)}
            else:
                if len(self.outputs) != 1:
                    raise ValueError(
                        f"{self} expected multiple outputs but output a {type(output)} which cannot have its length measured"
                    )
                return {list(self.outputs)[0]: output}
        except StopIteration:
            return None

    def set_extractor(self, *, connection=None, yield_per=None):
        extractor = self.setup(connection=connection, yield_per=yield_per)
        # Check if setup assigned the extractor already
        if extractor is not None:
            self._extractor = extractor
        elif getattr(self, 'extract', None) and callable(self.extract):
            self._extractor = self.extract()
        else:
            raise NotImplementedError(f"No way of getting the extractor from extract {self}")

    def __str__(self):
        return f"{self.__class__.__qualname__}<outputs= {self.outputs}>"
