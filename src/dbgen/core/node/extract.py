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
from typing import Mapping, Optional, TypeVar, Union

from pydantic import PrivateAttr

from dbgen.core.node.computational_node import ComputationalNode

extractor_type = GenType[Dict[str, Mapping[str, Any]], None, None]

T = TypeVar('T')
T1 = TypeVar('T1')

# TODO remove **_ from setup and fix tests
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
    def setup(self) -> None:
        pass

    def extract(self: 'Extract[T]') -> GenType[T, None, None]:
        yield {}  # type: ignore

    def teardown(self) -> None:
        pass

    def length(self) -> Optional[int]:
        return None

    # Internal Do not Overwrite

    def run(self, _: Dict[str, Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
        row = next(self._extractor)
        return self.process_row(row)

    def process_row(self, row):
        if row is None:
            return {}
        elif isinstance(row, Mapping):
            return dict(row)
        elif isinstance(row, tuple):
            l1, l2 = len(row), len(self.outputs)
            if l1 != l2:
                raise ValueError(
                    f"Expected {l2} output from extract {self}, but got {l1} outputs.\n"
                    f"If you intended to return a length {l1} tuple as the single output, please wrap the tuple in a singleton tuple\n"
                    "like so 'yield (tuple_to_return,)'"
                )
            return {name: val for name, val in zip(self.outputs, row)}
        else:
            if len(self.outputs) != 1:
                raise ValueError(
                    f"{self} expected multiple outputs but output a {type(row)} which cannot have its length measured"
                )
            return {list(self.outputs)[0]: row}

    def __enter__(self):
        """Call setup when extract used in with block."""
        self.setup()

    def __exit__(self, *_):
        """Call teardown when exiting extract with block regardless of error."""
        self.teardown()

    def __str__(self):
        return f"{self.__class__.__qualname__}<outputs= {self.outputs}>"
