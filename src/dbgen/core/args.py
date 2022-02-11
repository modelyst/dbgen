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

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

from dbgen.core.base import Base
from dbgen.exceptions import DBgenMissingInfo

if TYPE_CHECKING:
    from dbgen.core.func import Func  # pragma: no cover
    from dbgen.core.node.transforms import PythonTransform  # pragma: no cover

    Func


T = TypeVar('T')


class ArgLike(Base, Generic[T], metaclass=ABCMeta):
    @abstractmethod
    def arg_get(self, dic: dict):
        raise NotImplementedError

    def map(self, function: Callable[[Any], Any]) -> 'PythonTransform':
        from dbgen.core.node.transforms import PythonTransform

        return PythonTransform(inputs=[self], function=function)


class Arg(ArgLike[T]):
    """
    How a function refers to a namespace
    """

    key: str
    name: str

    def __str__(self) -> str:
        return f"Arg({str(self.key)[:4]}...,{self.name})"

    def __iter__(self):
        raise TypeError(
            f"You are attempting to iterate/unpack the arg object {self}. This can commonly occur when a Extract or Transform outputs a single output when you expected two. Did you remember to set the outputs on your Extract or Transform?"
        )

    def arg_get(self, namespace: dict) -> T:
        """
        Common interface for Const and Arg to get values out of namespace
        """
        try:
            val = namespace[self.key][self.name]
            return val
        except KeyError:
            if self.key not in namespace:
                raise DBgenMissingInfo(
                    f"could not find hash, looking for output named '{self.name}' at this hash {self.key}"
                )
            else:
                err = "could not find '%s' in %s "
                raise DBgenMissingInfo(err % (self.name, list(namespace[self.key].keys())))


class Constant(ArgLike[T]):
    val: T

    def __init__(self, val: T):
        super().__init__(val=val)

    def __str__(self) -> str:
        return f"Constant<{self.val}>"

    def arg_get(self, _: dict) -> T:
        return self.val
