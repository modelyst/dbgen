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
from typing import TYPE_CHECKING, Any, Callable

from dbgen.core.base import Base
from dbgen.core.func import Func
from dbgen.exceptions import DBgenMissingInfo

if TYPE_CHECKING:
    from dbgen.core.node.transforms import PyBlock


class ArgLike(Base, metaclass=ABCMeta):
    @abstractmethod
    def arg_get(self, dic: dict) -> Any:
        raise NotImplementedError

    def map(self, function: Callable[[Any], Any]) -> 'PyBlock':
        from dbgen.core.node.transforms import PyBlock

        return PyBlock(inputs=[self], function=function)


class Arg(ArgLike):
    """
    How a function refers to a namespace
    """

    key: str
    name: str

    def __str__(self) -> str:
        return f"Arg({str(self.key)[:4]}...,{self.name})"

    def arg_get(self, namespace: dict) -> Any:
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


class Const(ArgLike):
    val: Any

    def __init__(self, val: Any, *args, **kwargs) -> None:
        if callable(val):
            val = Func.from_callable(val)
        super().__init__(val=val, *args, **kwargs)

    def __str__(self) -> str:
        return f"Const<{self.val}>"

    def arg_get(self, _: dict) -> Any:
        return self.val
