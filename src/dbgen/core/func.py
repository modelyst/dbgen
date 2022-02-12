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

import ast
import inspect
import os
import re
from importlib.util import module_from_spec, spec_from_file_location
from inspect import (
    getdoc,
    getmembers,
    getsourcefile,
    getsourcelines,
    isbuiltin,
    isclass,
    isfunction,
    signature,
)
from os.path import exists
from pathlib import Path
from textwrap import dedent
from types import LambdaType
from typing import Any, Callable, ClassVar, Dict, Generic, List, Optional, Set, TypeVar, Union

from pydantic import Field, constr, root_validator, validator
from pydantic.fields import Undefined
from typing_extensions import ParamSpec

from dbgen.configuration import config
from dbgen.core.base import Base
from dbgen.exceptions import DBgenInternalError, InvalidArgument
from dbgen.utils.misc import reserved_words


class Import(Base):
    """
    Representation of an Python import line.

    Examples:

    --> from libname import unaliased, things, aliased as Thing
        Import('libname',         <---- (DO NOT make this a keyword argument!!!)
                'unaliased',
                'things',
                aliased = 'Thing')

    --> import numpy as np
        Import('numpy', alias = 'np')

    Unsupported edge case: trying to import some variable literally named
                            "alias" using an alias
    """

    lib: constr(min_length=1)  # type: ignore
    unaliased_imports: Union[str, List[str]] = Field(default_factory=list)
    lib_alias: Optional[str] = None
    aliased_imports: Dict[str, str] = Field(default_factory=dict)
    _reserved_words: ClassVar[Set[str]] = reserved_words

    def __init__(self, lib, unaliased_imports=None, lib_alias=None, aliased_imports=None):
        unaliased_imports = unaliased_imports or []
        aliased_imports = aliased_imports or {}
        super().__init__(
            lib=lib,
            lib_alias=lib_alias,
            unaliased_imports=unaliased_imports,
            aliased_imports=aliased_imports,
        )

    @root_validator(pre=True)
    def check_lib_alias(cls, values):
        terms = values.get("unaliased_imports") or values.get("aliased_imports")
        lib_alias = values.get("lib_alias")
        lib = values.get("lib")
        err = f"Can't import {lib} as {lib_alias} AND import specific terms ({terms}) at once"
        assert not (lib_alias and terms), err
        return values

    @validator("unaliased_imports", pre=True)
    def singleton_list(cls, unaliased_imports):
        if isinstance(unaliased_imports, str):
            return [unaliased_imports]
        else:
            return unaliased_imports or []

    @validator("aliased_imports")
    def reserved_words(cls, aliased_imports):
        for alias in aliased_imports.values():
            assert alias not in cls._reserved_words, f"Reserved python word used as alias: {alias}"
        return aliased_imports

    @validator("lib_alias")
    def check_reserbed_lib_alias(cls, lib_alias):
        assert lib_alias not in cls._reserved_words, f"Reserved python word used as lib_alias: {lib_alias}"
        return lib_alias

    def __str__(self) -> str:
        if not (self.unaliased_imports or self.aliased_imports):
            alias = f"as {self.lib_alias}" if self.lib_alias else ""
            return f"import {self.lib} {alias}"
        else:
            als = [f"{k} as {v}" for k, v in self.aliased_imports.items()]
            terms = list(self.unaliased_imports) + als
            return f"from {self.lib} import {', '.join(terms)}"

    def __lt__(self, other: Any) -> bool:
        if type(self) == type(other):
            return repr(self) < repr(other)
        raise TypeError(f"'<' not supported between instances of '{type(self)}' and '{type(other)}'")


class Environment(Base):
    """
    Environment in which a python statement gets executed
    """

    imports: List[Import] = Field(default_factory=lambda: list())

    def __init__(self, imports=None):
        if isinstance(imports, Import):
            imports = [imports]
        imports = imports or []
        super().__init__(imports=imports)

    @validator("imports")
    def sort_imports(cls, imports):
        return sorted(imports)

    def __str__(self) -> str:
        return "\n".join(map(str, self.imports))

    def __add__(self, other: "Environment") -> "Environment":
        return Environment(imports=list(set(self.imports + other.imports)))

    # Public methods #

    @staticmethod
    def from_str(import_string: str) -> "Environment":
        """Parse a header"""
        imports = []
        for node in ast.iter_child_nodes(ast.parse(import_string)):
            if isinstance(node, ast.ImportFrom):
                lib = node.module
                assert lib, f"Empty lib found {lib}\n{import_string}"
                aliased_imports = {}
                unaliased_imports = []
                for module in node.names:
                    if module.asname:
                        aliased_imports[module.name] = module.asname
                    else:
                        unaliased_imports.append(module.name)
                imports.append(
                    Import(
                        lib=lib,
                        unaliased_imports=unaliased_imports,
                        aliased_imports=aliased_imports,
                    )
                )
            elif isinstance(node, ast.Import):
                assert len(node.names) == 1, f"Bad Import String! {import_string}"
                lib = node.names[0].name
                lib_alias = node.names[0].asname or ""
                imports.append(Import(lib=lib, lib_alias=lib_alias))
        return Environment(imports=imports)

    @classmethod
    def from_file(cls, pth: Path) -> "Environment":
        with open(pth) as f:
            return cls.from_str(f.read())


Input = ParamSpec('Input')
Output = TypeVar('Output')
FuncIn = ParamSpec('FuncIn')
FuncOut = TypeVar('FuncOut')


class Func(Base, Generic[FuncOut]):
    """
    A function that can be used during the DB generation process.
    """

    src: str
    env: Environment

    def __str__(self) -> str:
        n = self.src.count("\n")
        s = "" if n == 1 else "s"
        return "<Func (%d line%s)>" % (n, s)

    def __call__(self, *args, **kwargs) -> FuncOut:
        if hasattr(self, "_func") and self.path.exists():
            return self._func(*args)
        else:
            f = self._from_src()
            return f(*args)

    def __repr__(self) -> str:
        return self.name

    # Properties #

    @property
    def name(self) -> str:
        return self._from_src().__name__

    @property
    def is_lam(self) -> bool:
        return self.src[:6] == "lambda"

    @property
    def doc(self) -> str:
        return getdoc(self._from_src()) or ""

    @property
    def sig(self) -> Any:
        return signature(self._from_src())

    @property
    def argnames(self) -> List[str]:
        return list(self.sig.parameters)

    @property
    def nIn(self) -> int:
        return len(self.argnames)

    @property
    def number_of_required_inputs(self) -> int:
        return len([k for k, v in self.sig.parameters.items() if v.default is v.empty])

    @property
    def notImp(self) -> bool:
        return "NotImplementedError" in self.src

    @property
    def output(self) -> Any:
        return self.sig.return_annotation

    @property
    def nOut(self) -> int:
        return 1

    # @property
    # def inTypes(self) -> List["DataType"]:
    #     return [
    #         DataType.get_datatype(x.annotation) for x in self.sig.parameters.values()
    #     ]

    @property
    def path(self) -> Path:
        return config.temp_dir / f"{self.hash}.py"

    # @property
    # def outTypes(self) -> List["DataType"]:
    #     ot = DataType.get_datatype(self.output)
    #     if len(ot) == 1:
    #         return [ot]
    #     else:
    #         assert isinstance(ot, Tuple)
    #         return ot.args

    def file(self) -> str:
        lam = "f = " if self.is_lam else ""
        return str(self.env) + "\n" + lam + self.src

    # Private methods #

    def _from_src(self, force: bool = False) -> Callable:
        """
        Execute source code to get a callable
        """

        if force or not exists(self.path):
            with open(self.path, "w") as t:
                t.write(self.file())

        f = self.path_to_func(str(self.path))

        return f

    # Public methods #
    def store_func(self, force: bool = False) -> None:
        """Load func from source code and store as attribute (better performance
        but object is no longer serializable / comparable for equality )
        """
        self._func = self._from_src(force)

    @staticmethod
    def path_to_func(pth: str) -> Callable:

        try:
            spec = spec_from_file_location("random", pth)
            assert spec and spec.loader, "Spec or Spec.loader are broken"
            mod = module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore
            transforms = [o for o in getmembers(mod) if isfunction(o[1]) and getsourcefile(o[1]) == pth]
            assert len(transforms) == 1, "Bad input file %s has %d functions, not 1" % (
                pth,
                len(transforms),
            )
            return transforms[0][1]

        except Exception as e:
            if exists(pth):
                with open(pth) as f:
                    content = f.read()
            raise DBgenInternalError(
                f"Error while trying to load source code. You may be missing an import in your Transforms Env object. \nPath:{pth}\nFile Contents:\n--------\n{content}\n--------\nLoad Error: {e}"
            )


def get_short_lambda_source(lambda_func):
    """Return the source of a (short) lambda function.
    If it's impossible to obtain, returns None.
    """
    try:
        source_lines, _ = inspect.getsourcelines(lambda_func)
    except (OSError, TypeError):
        return None

    # skip `def`-ed functions and long lambdas
    if len(source_lines) != 1:
        return None

    source_text = os.linesep.join(source_lines).strip()

    # find the AST node of a lambda definition
    # so we can locate it in the source code
    try:
        source_ast = ast.parse(source_text)
    except SyntaxError:
        return None
    lambda_node = next((node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)), None)
    if lambda_node is None:  # could be a single line `def fn(x): ...`
        return None

    # HACK: Since we can (and most likely will) get source lines
    # where lambdas are just a part of bigger expressions, they will have
    # some trailing junk after their definition.
    #
    # Unfortunately, AST nodes only keep their _starting_ offsets
    # from the original source, so we have to determine the end ourselves.
    # We do that by gradually shaving extra junk from after the definition.
    lambda_text = source_text[lambda_node.col_offset :]
    lambda_body_text = source_text[lambda_node.body.col_offset :]
    min_length = len("lambda:_")  # shortest possible lambda expression
    while len(lambda_text) > min_length:
        try:
            # What's annoying is that sometimes the junk even parses,
            # but results in a *different* lambda. You'd probably have to
            # be deliberately malicious to exploit it but here's one way:
            #
            #     bloop = lambda x: False, lambda x: True
            #     get_short_lamnda_source(bloop[0])
            #
            # Ideally, we'd just keep shaving until we get the same code,
            # but that most likely won't happen because we can't replicate
            # the exact closure environment.
            code = compile(lambda_body_text, "<unused filename>", "eval")

            # Thus the next best thing is to assume some divergence due
            # to e.g. LOAD_GLOBAL in original code being LOAD_FAST in
            # the one compiled above, or vice versa.
            # But the resulting code should at least be the same *length*
            # if otherwise the same operations are performed in it.
            if len(code.co_code) == len(lambda_func.__code__.co_code):
                return lambda_text
        except SyntaxError:
            pass
        lambda_text = lambda_text[:-1]
        lambda_body_text = lambda_body_text[:-1]

    return None


lambda_pattern = r"\s*(\w+)\s*=\s*(lambda.*)"
lambda_regex = re.compile(lambda_pattern)

# TODO parsing functions with multiline decoration
def get_callable_source_code(f: Callable) -> str:
    """
    Return the source code, even if it's lambda function.
    """
    # Check for built in functions as their source code can not be fetched
    if isbuiltin(f) or (isclass(f) and getattr(f, "__module__", "") == "builtins"):
        raise InvalidArgument(
            f"Error getting source code for transform. {f} is a built-in function.\n"
            f"Please wrap in lambda like so: `lambda x: {f.__name__}(x)`"
        )

    try:
        source_lines, _ = getsourcelines(f)
    except (OSError, TypeError) as exc:
        # functions defined in pdb / REPL / eval / some other way in which
        # source code not clear
        error = (
            f"Cannot parse function {f.__name__}. This can occur if python cannot find the file the function was originally declared in.\n"
            "This can occur commonly the function's file is mounted to the filesystem (through a docker volume).\n"
            "To solve this copy the file into the container."
        )
        raise ValueError(error) from exc

    if isinstance(f, LambdaType) and f.__name__ == "<lambda>":
        source_code = get_short_lambda_source(f)
        if source_code:
            return source_code

    # If we have regular def function() type function we need to parse out any decorators
    # to do this we parse the function with ast and remove any lines that are not relevant
    source_code = dedent(''.join(source_lines))
    source_ast = ast.parse(source_code)
    # Get the first FunctionDef node by walking through the parsed ast tree
    function_node = next((node for node in ast.walk(source_ast) if isinstance(node, ast.FunctionDef)), None)
    if function_node is None:
        raise ValueError(f"Can't parse function:\n{source_code}")
    src = ''.join(source_lines[function_node.lineno - 1 : function_node.end_lineno])
    src = dedent(src).strip()
    if len(source_lines) > 1 and src[:3] == "def":
        return src

    match = lambda_regex.match(src)
    if match:
        func_name, func = match.groups()
        func_name = func_name.strip()
        func = func.strip()
    else:
        raise ValueError(f"Can't parse lambda:\n{src}")

    # Slice off trailing chars until we get a callable function
    try:
        eval_func = eval(func)
    except SyntaxError:
        raise ValueError(f"Cannot parse the source code due to syntax error:\n#####\n{func}")
    assert callable(eval_func)
    return func


def func_from_callable(func_: Callable[..., Output], env: Optional[Environment] = None) -> Func[Output]:
    """Generate a func from a variety of possible input data types.

    Args:
        func_ (Callable[..., Output]): Python function to convert to Func object
        env (Optional[Env], optional): Env object to include on the Function. Defaults to None.

    Returns:
        Func[Output]: Output Func object wrapped around the input function
    """
    if isinstance(env, dict):
        env = Environment.from_dict(env)
    elif env is None or env is Undefined:
        env = Environment()
    if isinstance(func_, Func):
        # assert not getattr(env,'imports',False)
        return Func(src=func_.src, env=env)
    else:
        assert callable(func_), f"tried to instantiate Func, but not callable {type(func_)}"
        return Func(src=get_callable_source_code(func_), env=env)
