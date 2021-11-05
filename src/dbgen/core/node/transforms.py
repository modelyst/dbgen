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
from typing import Any, Callable, Dict, Generic, List, Mapping, Optional, TypeVar, Union, cast, overload

from pydantic import Field, validator
from pydantic.class_validators import root_validator
from pydantic.fields import Undefined
from typing_extensions import ParamSpec

from dbgen.core.args import Arg, Const
from dbgen.core.func import Env, Func, func_from_callable
from dbgen.core.node.computational_node import ComputationalNode
from dbgen.exceptions import DBgenExternalError, DBgenPyBlockError, DBgenSkipException
from dbgen.utils.log import capture_stdout

Output = TypeVar('Output')


class Transform(ComputationalNode[Output]):
    pass


class PyBlock(Transform[Output]):

    function: Func[Output]
    env: Optional[Env] = Field(default_factory=lambda: Env(imports=set()))

    def __init__(
        self, function: Callable[..., Output], env: Env = Undefined, inputs=Undefined, outputs=Undefined
    ):
        # add explicit __init__ to allow for positional args
        assert callable(function) or isinstance(function, Func) or isinstance(function, dict)
        if callable(function):
            function = func_from_callable(function, env=env)
        kwargs = {
            k: val
            for k, val in (('env', env), ('inputs', inputs), ('outputs', outputs))
            if val is not Undefined and val is not None
        }
        super().__init__(function=function, **kwargs)

    def run(self, namespace_dict: Dict[str, Mapping[str, Any]]) -> Dict[str, Any]:
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
        if "function" not in values:
            return values
        inputs = values.get("inputs")
        num_req_args = values.get("function").number_of_required_inputs
        num_max_args = values.get("function").nIn
        n_inputs = len(inputs)
        assert (
            n_inputs >= num_req_args
        ), f"Too few arguments supplied to Func:\nNumber of Inputs: {n_inputs}\nNumber of Args: {num_req_args}\n{values}"
        assert (
            n_inputs <= num_max_args
        ), f"Too many arguments supplied to Func:\nNumber of Inputs: {n_inputs}\nMax Number of Args: {num_max_args}\n"
        return values

    def __call__(self, *args, **kwargs) -> Output:
        return self.function(*args, **kwargs)
