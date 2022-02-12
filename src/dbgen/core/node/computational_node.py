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

from pathlib import Path
from typing import Any, Dict, Generic, List, Mapping, Optional, Tuple, TypeVar, Union, overload

from pydantic import Field, validator

from dbgen.core.args import Arg, ArgLike, Constant
from dbgen.core.base import Base
from dbgen.core.context import ETLStepContext
from dbgen.core.dependency import Dependency
from dbgen.exceptions import DBgenMissingInfo

BasicType = TypeVar('BasicType', int, float, str, bytes, Path)
T1 = TypeVar('T1')
T2 = TypeVar('T2')
T3 = TypeVar('T3')
T4 = TypeVar('T4')
T5 = TypeVar('T5')
Output = TypeVar('Output')


class ComputationalNode(Base, Generic[Output]):
    inputs: Mapping[str, Union[Constant, Arg]] = Field(default_factory=lambda: {})
    outputs: List[str] = Field(default_factory=lambda: ["out"], min_items=1)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        etl_step_context = ETLStepContext.get()
        if etl_step_context:
            etl_step = etl_step_context['etl_step']
            etl_step.add_node(self)

    def _get_dependency(self) -> Dependency:
        return Dependency()

    @validator("inputs", pre=True)
    def convert_list_to_dict(cls, inputs):
        if isinstance(inputs, dict):
            return inputs
        new_inputs = {}
        for arg_idx, arg_val in enumerate(inputs):
            if isinstance(arg_val, (str, int, bool, float)):
                new_inputs[str(arg_idx)] = Constant(val=arg_val)
            elif isinstance(arg_val, ArgLike):
                new_inputs[str(arg_idx)] = arg_val
            else:
                raise TypeError(f"Bad input type provided: {type(arg_val)} {arg_val}")
        return new_inputs

    @validator("outputs", pre=True)
    def unique_keys(cls, outputs):
        if isinstance(outputs, set):
            raise ValueError(
                f"Outputs cannot be a set as order cannot be preserved...{outputs}",
            )
        elif isinstance(outputs, (list, tuple)):
            if len(set(outputs)) != len(outputs):
                raise ValueError(["No duplicate output names allowed"])
        return outputs

    @property
    def name(self):
        return str(self)

    def __getitem__(self, key: str):
        assert key in self.outputs, f"Key {key} not found: {self.outputs}"
        return Arg(key=self.hash, name=key)

    def _get_inputs(self, namespace: Dict[str, Mapping[str, Any]]) -> Dict[str, Any]:
        try:
            input_variables = {name: value.arg_get(namespace) for name, value in self.inputs.items()}
        except (TypeError, IndexError) as e:
            print(e)
            print(namespace)
            raise ValueError()
        except AttributeError:
            invalid_args = [getattr(arg, "arg_get", None) is None for arg in self.inputs]
            missing_args = filter(lambda x: invalid_args[x], range(len(invalid_args)))
            raise DBgenMissingInfo(
                f"Argument(s) {' ,'.join(map(str,missing_args))} to {self.name} don't have arg_get attribute:\n Did you forget to wrap a Const around a transform Argument?"
            )
        return input_variables

    def run(self, namespace: Dict[str, Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
        return {}

    @overload
    def results(
        self: 'ComputationalNode[Tuple[T1,T2,T3,T4,T5]]',
    ) -> Tuple[Arg[T1], Arg[T2], Arg[T3], Arg[T4], Arg[T5]]:
        ...

    @overload
    def results(self: 'ComputationalNode[Tuple[T1,T2,T3,T4]]') -> Tuple[Arg[T1], Arg[T2], Arg[T3], Arg[T4]]:
        ...

    @overload
    def results(self: 'ComputationalNode[Tuple[T1,T2,T3]]') -> Tuple[Arg[T1], Arg[T2], Arg[T3]]:
        ...

    @overload
    def results(self: 'ComputationalNode[Tuple[T1,T2]]') -> Tuple[Arg[T1], Arg[T2]]:
        ...

    @overload
    def results(self: 'ComputationalNode[Tuple[T1]]') -> Arg[T1]:
        ...

    @overload
    def results(self: 'ComputationalNode[BasicType]') -> Arg[BasicType]:
        ...

    def results(self):
        arglist = tuple(iter(map(self.__getitem__, self.outputs)))
        return arglist[0] if len(arglist) == 1 else arglist
