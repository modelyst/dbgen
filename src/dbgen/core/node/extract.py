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

from typing import TYPE_CHECKING, Any, Callable, Dict
from typing import Generator
from typing import Generator as GenType
from typing import Mapping, Optional, TypeVar, Union

from pydantic import Field, PrivateAttr, root_validator, validator

from dbgen.core.func import Environment, Func, func_from_callable
from dbgen.core.node.computational_node import ComputationalNode

if TYPE_CHECKING:
    from dbgen.core.run.utilities import RunConfig
extractor_type = GenType[Dict[str, Mapping[str, Any]], None, None]

Output = TypeVar('Output')
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
    _run_config: 'RunConfig' = PrivateAttr(None)

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

    def _set_run_config(self, run_config: 'RunConfig'):
        self._run_config = run_config

    def run(
        self, _: Dict[str, Mapping[str, Any]], run_config: Optional['RunConfig'] = None
    ) -> Optional[Dict[str, Any]]:
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


# TODO add better error messaging when user passes in a non-arg to pyblock
class PythonExtract(Extract[Output]):

    env: Optional[Environment] = Field(default_factory=lambda: Environment(imports=set()))
    function: Func[Output]

    @validator('function', pre=True)
    def convert_callable_to_func(cls, function: Union[Func[Output], Callable[..., Output]], values):
        env = values.get('env', Environment())
        if isinstance(function, (Func, dict)):
            return function
        elif callable(function):
            return func_from_callable(function, env=env)
        raise ValueError(f"Unknown function type {type(function)} {function}")

    @root_validator
    def check_nargs(cls, values):
        if "function" not in values:
            return values
        inputs = values.get("inputs")
        func = values.get("function")
        num_req_args = values.get("function").number_of_required_inputs
        num_max_args = values.get("function").number_of_inputs
        if inputs:
            number_of_inputs = len(inputs)
            assert (
                number_of_inputs >= num_req_args
            ), f"Too few arguments supplied to Func. Number of Inputs: {number_of_inputs}, Number of Args: {num_req_args}\n{values}"
            if not func.var_positional_keyword:
                assert (
                    number_of_inputs <= num_max_args
                ), f"Too many arguments supplied to Func. Number of Inputs: {number_of_inputs}, Max Number of Args: {num_max_args}\n"
        return values

    def extract(self) -> Generator[Output, None, None]:
        inputvars = self._get_inputs({})
        args = {key: val for key, val in inputvars.items() if key.isdigit()}
        kwargs = {key: val for key, val in inputvars.items() if key not in args}
        if 'settings' in self.function.argnames:
            setting_index = self.function.argnames.index('settings')
            if 'settings' not in kwargs or len(args) < setting_index + 1:
                kwargs['settings'] = self._run_config.settings

        yield from self.function(*args.values(), **kwargs)  # type: ignore
