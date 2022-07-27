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
import inspect
from functools import partial
from typing import Callable, Generic, List, Optional, Tuple, TypeVar, Union, overload

from pydantic import ValidationError
from pydantic.typing import get_args, get_origin
from typing_extensions import ParamSpec

from dbgen.core.args import Arg
from dbgen.core.func import Environment
from dbgen.core.node.computational_node import ComputationalNode
from dbgen.core.node.extract import PythonExtract
from dbgen.core.node.transforms import PythonTransform
from dbgen.exceptions import InvalidArgument

In = ParamSpec('In')
Out = TypeVar('Out')
T = TypeVar('T')
T1 = TypeVar('T1')
T2 = TypeVar('T2')
T3 = TypeVar('T3')
T4 = TypeVar('T4')


class FunctionNode(Generic[In, Out]):
    """Temporary wrapper API for pyblocks."""

    def __init__(
        self,
        *inputs,
        function: Callable[In, Out] = None,
        env: Optional[Environment] = None,
        outputs=None,
        node: Optional[ComputationalNode] = None,
    ):
        self.function = function
        self.env = env
        self.inputs = inputs or []
        self.outputs = outputs or ['out']
        self.node = node
        self._arglist = tuple(iter(map(node.__getitem__, self.node.outputs)))

    def __call__(self: 'FunctionNode[In,Out]', *args: In.args, **kwargs: In.kwargs) -> Out:
        return self.function(*args, **kwargs)

    def __getitem__(self: 'FunctionNode[In,Out]', key: Union[str, int]) -> Arg:
        if isinstance(key, str):
            return self.node[key]
        return self._arglist[key]

    @overload
    def results(self: 'FunctionNode[In,Tuple[T1]]') -> Tuple[Arg[T1]]:
        ...  # pragma: no cover

    @overload
    def results(self: 'FunctionNode[In,Tuple[T1,T2]]') -> Tuple[Arg[T1], Arg[T2]]:
        ...  # pragma: no cover

    @overload
    def results(self: 'FunctionNode[In,Tuple[T1,T2,T3]]') -> Tuple[Arg[T1], Arg[T2], Arg[T3]]:
        ...  # pragma: no cover

    @overload
    def results(self: 'FunctionNode[In,T1]') -> Arg[T1]:
        ...  # pragma: no cover

    @overload
    def results(
        self: 'FunctionNode[In,Tuple[T1,T2,T3,T4]]',
    ) -> Tuple[Arg[T1], Arg[T2], Arg[T3], Arg[T4]]:
        ...  # pragma: no cover

    def results(self):
        return self._arglist[0] if len(self._arglist) == 1 else self._arglist


class TransformNode(FunctionNode[In, Out]):
    """Temporary wrapper API for pyblocks."""

    def __init__(
        self,
        *inputs,
        function: Callable[In, Out] = None,
        env: Optional[Environment] = None,
        outputs=None,
        kwargs: dict = None,
    ):
        outputs = outputs or ['out']
        kwargs = kwargs or {}
        try:
            node = PythonTransform(function=function, env=env, inputs=inputs, kwargs=kwargs, outputs=outputs)
        except ValidationError as exc:
            raise InvalidArgument(
                f'Error occurred during the validation of the transform {function.__name__!r}'
            ) from exc
        super().__init__(*inputs, function=function, env=env, outputs=outputs, node=node)


class ExtractNode(FunctionNode[In, Out]):
    """Temporary wrapper API for pyblocks."""

    def __init__(
        self,
        *inputs,
        function: Callable[In, Out] = None,
        env: Optional[Environment] = None,
        outputs=None,
    ):
        outputs = outputs or ['out']

        try:
            node = PythonExtract(function=function, env=env, inputs=inputs, outputs=outputs)
        except ValidationError as exc:
            raise InvalidArgument(
                f'Error occurred during the validation of the transform {function.__name__!r}'
            ) from exc
        super().__init__(*inputs, function=function, env=env, outputs=outputs, node=node)


@overload
def transform(function: Callable[In, Out]) -> Callable[In, FunctionNode[In, Out]]:
    ...  # pragma: no cover


@overload
def transform(
    *, env: Environment = None, outputs: List[str] = None
) -> Callable[[Callable[In, Out]], Callable[In, FunctionNode[In, Out]]]:
    ...  # pragma: no cover


def transform(function=None, *, env: Optional[Environment] = None, outputs: List[str] = None):

    if function:
        if not outputs:

            # TODO add dict outputs or list of dict outputs!
            sig = inspect.signature(function)
            if outputs is None and sig.return_annotation:
                annotation = sig.return_annotation
                origin = get_origin(annotation)
                if origin is not None and origin is not Union and issubclass(origin, (list, tuple)):
                    args = get_args(annotation)
                    bad_args = list(filter(lambda x: not isinstance(x, type), args))
                    if not bad_args:
                        outputs = [str(i) for i, _ in enumerate(args)]
        func = partial(TransformNode, function=function, env=env, outputs=outputs)

        def set_inputs(*inputs: List[Arg], **kwargs) -> FunctionNode[In, Out]:
            return func(*inputs, kwargs=kwargs)

        return set_inputs
    else:
        return partial(transform, env=env, outputs=outputs)


@overload
def extract(function: Callable[In, Out]) -> Callable[In, FunctionNode[In, Out]]:
    ...  # pragma: no cover


@overload
def extract(
    *, env: Environment = None, outputs: List[str] = None
) -> Callable[[Callable[In, Out]], Callable[In, FunctionNode[In, Out]]]:
    ...  # pragma: no cover


def extract(function=None, *, env: Optional[Environment] = None, outputs: List[str] = None):

    if function:
        if not outputs:

            # TODO add dict outputs or list of dict outputs!
            sig = inspect.signature(function)
            if outputs is None and sig.return_annotation:
                annotation = sig.return_annotation
                origin = get_origin(annotation)
                if origin is not None and origin is not Union and issubclass(origin, (list, tuple)):
                    args = get_args(annotation)
                    bad_args = list(filter(lambda x: not isinstance(x, type), args))
                    if not bad_args:
                        outputs = [str(i) for i, _ in enumerate(args)]
        func = partial(ExtractNode, function=function, env=env, outputs=outputs)

        def set_inputs(*inputs: List[Arg], **kwargs) -> ExtractNode[In, Out]:
            return func(*inputs, kwargs=kwargs)

        return set_inputs
    else:
        return partial(extract, env=env, outputs=outputs)
