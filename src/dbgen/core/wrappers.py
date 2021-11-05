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

from functools import partial
from typing import (
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

from pydantic.main import Undefined
from typing_extensions import ParamSpec

from dbgen.core.args import Arg
from dbgen.core.func import Env
from dbgen.core.node.transforms import PyBlock

In = ParamSpec('In')
Out = TypeVar('Out')
T = TypeVar('T')
T1 = TypeVar('T1')
T2 = TypeVar('T2')
T3 = TypeVar('T3')
T4 = TypeVar('T4')
TDict = TypeVar('TDict', bound=TypedDict)


class TypeArg(Arg, Generic[T]):
    pass


class FunctionNode(Generic[In, Out]):
    """Temporary wrapper API for pyblocks."""

    def __init__(
        self,
        *inputs,
        function: Callable[In, Out] = None,
        env: Optional[Env] = None,
        outputs=None,
    ):
        self.function = function
        self.env = env
        self.inputs = inputs or []
        self.outputs = outputs or ['out']
        self.pyblock = self.to_pyblock()
        self._arglist: 'Out' = tuple(iter(self))

    def __call__(self: 'FunctionNode[In,Out]', *args: In.args, **kwargs: In.kwargs) -> Out:
        return self.function(*args, **kwargs)

    @overload
    def __iter__(self: 'FunctionNode[In,Tuple[T1]]') -> Iterable[TypeArg[T1]]:
        ...

    def __iter__(self: 'FunctionNode[In,Out]') -> Iterable['Arg']:
        return iter(map(self.pyblock.__getitem__, self.pyblock.outputs))

    def __getitem__(self: 'FunctionNode[In,Out]', key: Union[str, int]):
        if isinstance(key, str):
            return self.pyblock[key]

        return self._arglist[key]

    @overload
    def results(self: 'FunctionNode[In,Tuple[T1]]') -> Tuple[TypeArg[T1]]:
        ...

    @overload
    def results(self: 'FunctionNode[In,Tuple[T1,T2]]') -> Tuple[TypeArg[T1], TypeArg[T2]]:
        ...

    @overload
    def results(self: 'FunctionNode[In,Tuple[T1,T2,T3]]') -> Tuple[TypeArg[T1], TypeArg[T2], TypeArg[T3]]:
        ...

    @overload
    def results(self: 'FunctionNode[In,T1]') -> TypeArg[T1]:
        ...

    @overload
    def results(
        self: 'FunctionNode[In,Tuple[T1,T2,T3,T4]]',
    ) -> Tuple[TypeArg[T1], TypeArg[T2], TypeArg[T3], TypeArg[T4]]:
        ...

    # @overload
    # def results(self: 'FunctionNode[In,T1]') -> T1:
    #     ...

    def results(self):
        out = tuple(iter(self))
        return out[0] if len(out) == 1 else out

    def to_pyblock(self):
        return PyBlock(function=self.function, env=self.env, inputs=self.inputs, outputs=self.outputs)


@overload
def node(function: Callable[In, Out]) -> Callable[In, FunctionNode[In, Out]]:
    ...


@overload
def node(
    *, env: Env = None, outputs: List[str] = None
) -> Callable[[Callable[In, Out]], Callable[In, FunctionNode[In, Out]]]:
    ...


def node(function=None, *, env: Optional[Env] = None, outputs: List[str] = None):

    if function:
        func = partial(FunctionNode, function=function, env=env, outputs=outputs)

        def set_inputs(*inputs: List[Arg]) -> FunctionNode[In, Out]:
            return func(*inputs)

        return set_inputs
    else:
        return partial(node, env=env, outputs=outputs)
