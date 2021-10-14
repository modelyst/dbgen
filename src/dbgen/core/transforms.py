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

from traceback import format_exc
from typing import Any, Callable, Dict, List, Mapping, Union, cast

from pydantic import Field, validator
from pydantic.class_validators import root_validator

from dbgen.core.args import Arg, Const
from dbgen.core.computational_node import ComputationalNode
from dbgen.core.func import Env, Func
from dbgen.exceptions import DBgenExternalError, DBgenPyBlockError, DBgenSkipException
from dbgen.utils.log import capture_stdout


class Transform(ComputationalNode):
    pass


class PyBlock(Transform):
    env: Env = Field(default_factory=lambda: Env(imports=set()))
    function: Func

    @validator("function", pre=True)
    def convert_callable(cls, func, values):
        assert callable(func) or isinstance(func, Func) or isinstance(func, dict)
        if callable(func):
            return Func.from_callable(func, env=values.get("env"))
        return func

    def run(self, namespace_dict: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        inputvars = self._get_inputs(namespace_dict)
        args = {key: val for key, val in inputvars.items() if key.isdigit()}
        kwargs = {key: val for key, val in inputvars.items() if key not in args}
        try:
            wrapped = capture_stdout(self.function)
            output = wrapped(*args.values(), **kwargs)
            if isinstance(output, tuple):
                l1, l2 = len(output), len(self.outputs)
                assert l1 == l2, "Expected %d outputs from %s, got %d" % (
                    l2,
                    self.function.name,
                    l1,
                )
                return {o: val for val, o in zip(output, self.outputs)}
            else:
                if len(self.outputs) != 1:
                    raise DBgenPyBlockError(
                        f"Function returned a non-tuple but outputs is greater length 1: {self.outputs}"
                    )
                return {list(self.outputs)[0]: output}
        except (DBgenSkipException, DBgenPyBlockError):
            raise
        except Exception:
            msg = f"Error encountered while applying function named {self.function.name!r}:\n\t"
            raise DBgenExternalError(msg + format_exc())

    @root_validator
    def check_nargs(cls, values):
        assert "function" in values, f"function not found in values {values}"
        args = values.get("inputs")
        n_inputs = values.get("function").nIn
        n_args = len(args)
        assert (
            n_inputs == n_args
        ), f"Unequal args and inputs required:\nNumber of Inputs: {n_inputs}\nNumber of Args: {n_args}\n"
        return values


def apply_pyblock(
    env: Env = None,
    inputs: Union[Mapping[str, Union[Const, Arg]], List[Union[Const, Arg]]] = None,
    outputs: List[str] = None,
) -> Callable[[Callable], PyBlock]:
    inputs = inputs or {}
    outputs = outputs or ["out"]
    env = env if env is not None else Env()
    env = cast(Env, env)

    def wrapper(function: Callable) -> PyBlock:
        func = Func.from_callable(function, env=env)
        return PyBlock(function=func, inputs=inputs, outputs=outputs, env=env)

    return wrapper
