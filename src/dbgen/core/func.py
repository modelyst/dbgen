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
from re import findall
from sys import version_info

# External Modules
from typing import Any
from typing import Callable as C
from typing import Dict as D
from typing import List as L
from typing import Union as U

from hypothesis.strategies import SearchStrategy, builds

# Iternal Modules
from dbgen.core.datatypes import DataType, Tuple
from dbgen.utils.config import DBGEN_TMP
from dbgen.utils.exceptions import DBgenInternalError, DBgenInvalidArgument
from dbgen.utils.misc import Base, hash_
from dbgen.utils.sql import Connection as Conn
from dbgen.utils.sql import mkInsCmd, mkSelectCmd, sqlexecute, sqlselect

"""
Defines the Func class, which is initialized with any Python callable but gets
enriched in the __init__ with a lot more information and methods (from inspect)
"""
assert version_info[1] > 5, "Stop using old python3 (need 3.x, x > 5)"
################################################################################
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

    def __init__(
        self,
        lib: str,
        unaliased_imports: U[L[str], str] = None,
        lib_alias: str = "",
        aliased_imports: D[str, str] = None,
    ) -> None:

        err = "Can't import %s as %s AND import specific terms (%s,%s) at once"
        terms = unaliased_imports or aliased_imports
        assert not (lib_alias and terms), err % (
            lib,
            lib_alias,
            unaliased_imports,
            aliased_imports,
        )

        self.lib = lib
        self.lib_alias = lib_alias

        if isinstance(unaliased_imports, str):
            self.unaliased_imports = [unaliased_imports]
        else:
            self.unaliased_imports = unaliased_imports or []
        self.aliased_imports = aliased_imports or {}
        super().__init__()

    def __str__(self) -> str:
        if not (self.unaliased_imports or self.aliased_imports):
            alias = f"as {self.lib_alias}" if self.lib_alias else ""
            return f"import {self.lib} {alias}"
        else:
            als = [f"{k} as {v}" for k, v in self.aliased_imports.items()]
            terms = list(self.unaliased_imports) + als
            return f"from {self.lib} import {', '.join(terms)}"

    def __eq__(self, other: object) -> bool:
        return False if not isinstance(other, Import) else vars(self) == vars(other)

    def __hash__(self) -> int:
        return int(self.hash)

    def __lt__(self, other: Any) -> bool:
        if type(self) == type(other):
            return repr(self) < repr(other)
        raise TypeError(f"'<' not supported between instances of '{type(self)}' and '{type(other)}'")

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)

    @staticmethod
    def from_str(s: str) -> "Import":
        """Parse a header line (parens not supported yet)"""
        if s[:6] == "import":
            if " as " in s[7:]:
                lib, lib_alias = [s[7:].split("as")[i].strip() for i in range(2)]
                return Import(lib, lib_alias=lib_alias)
            return Import(s.split()[1].strip())
        else:
            i = s.find("import")
            a, lib = s[:i].split()
            assert a == "from", "Bad source code beginning : \n\n\n" + s
            pat = r"([a-zA-Z0-9\_]+\s*(?:as\s*[a-zA-Z0-9\_]+)?)"
            groups = findall(pat, s[i + 6 :])
            objs = [list(map(str.strip, g.split("as"))) for g in groups]
            # objs_ = [x[0] if len(x) == 1 else tuple(x) for x in objs]
            unalias = [x[0] for x in objs if len(x) == 1]
            aliased = {x[0]: x[1] for x in objs if len(x) == 2}

            return Import(lib, unaliased_imports=unalias, aliased_imports=aliased)


class Env(Base):
    """
    Environment in which a python statement gets executed
    """

    def __init__(self, imports: L[Import] = None) -> None:

        if imports:
            assert isinstance(
                imports, list
            ), "Env takes in a list of imports. If there is 1 import wrap it in a list"

            self.imports = sorted(imports)
        else:
            self.imports = []
        super().__init__()

    def __str__(self) -> str:
        return "\n".join(map(str, self.imports))

    def __add__(self, other: "Env") -> "Env":
        return Env(list(set(self.imports + other.imports)))

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)

    # Public methods #

    @staticmethod
    def from_str(import_string: str) -> "Env":
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
                        lib,
                        unaliased_imports=unaliased_imports,
                        aliased_imports=aliased_imports,
                    )
                )
            elif isinstance(node, ast.Import):
                assert len(node.names) == 1, f"Bad Import String! {import_string}"
                lib = node.names[0].name
                lib_alias = node.names[0].asname or ""
                imports.append(Import(lib, lib_alias=lib_alias))
        return Env(imports)

    @classmethod
    def from_file(cls, pth: Path) -> "Env":
        with open(pth) as f:
            return cls.from_str(f.read())


################################################################################
class Func(Base):
    """
    A function that can be used during the DB generation process.
    """

    def __init__(self, src: str, env: Env) -> None:
        assert isinstance(src, str), f"Expected src str, but got {type(src)}"
        assert isinstance(env, Env), f"Expected Env, but got {type(env)}"

        self.src = src
        self.env = env
        super().__init__()

    def __str__(self) -> str:
        n = self.src.count("\n")
        s = "" if n == 1 else "s"
        return "<Func (%d line%s)>" % (n, s)

    def __call__(self, *args: Any) -> Any:
        if hasattr(self, "_func") and self.path.exists():
            return self._func(*args)
        else:
            f = self._from_src()
            return f(*args)

    def __repr__(self) -> str:
        return self.name

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)

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
    def argnames(self) -> L[str]:
        return list(self.sig.parameters)

    @property
    def nIn(self) -> int:
        return len(self.inTypes)

    @property
    def notImp(self) -> bool:
        return "NotImplementedError" in self.src

    @property
    def output(self) -> Any:
        return self.sig.return_annotation

    @property
    def nOut(self) -> int:
        return len(self.outTypes)

    @property
    def inTypes(self) -> L[DataType]:
        return [DataType.get_datatype(x.annotation) for x in self.sig.parameters.values()]

    @property
    def path(self) -> Path:
        return DBGEN_TMP / (str(hash_(self.file())) + ".py")

    @property
    def outTypes(self) -> L[DataType]:
        ot = DataType.get_datatype(self.output)
        if len(ot) == 1:
            return [ot]
        else:
            assert isinstance(ot, Tuple)
            return ot.args

    def file(self) -> str:
        lam = "f = " if self.is_lam else ""
        return str(self.env) + "\n" + lam + self.src

    # Private methods #

    def _from_src(self, force: bool = False) -> C:
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

    def del_func(self) -> None:
        """Remove callable attribute after performance is no longer needed"""
        if hasattr(self, "_func"):
            del self._func

    def add(self, cxn: Conn) -> int:
        """
        Log function data to metaDB, return its ID
        """
        q = mkSelectCmd("_func", ["func_id"], ["checksum"])
        f_id = sqlselect(cxn, q, [hash_(self.src)])
        if f_id:
            return f_id[0][0]
        else:

            cols = [
                "name",
                "checksum",
                "source",
                "docstring",
                "inTypes",
                "outType",
                "n_in",
                "n_out",
            ]

            binds = [
                self.name,
                hash_(self.src),
                self.src,
                self.doc,
                str(self.inTypes),
                str(self.outTypes),
                self.nIn,
                self.nOut,
            ]

            sqlexecute(cxn, mkInsCmd("_func", cols), binds)
            f_id = sqlselect(cxn, q, [hash_(self.src)])
            return f_id[0][0]

    @staticmethod
    def path_to_func(pth: str) -> C:

        try:
            spec = spec_from_file_location("random", pth)
            mod = module_from_spec(spec)
            assert spec and spec.loader, "Spec or Spec.loader are broken"
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
                f"Error while trying to load source code. You may be missing an import in your PyBlocks Env object. \nPath:{pth}\nFile Contents:\n--------\n{content}\n--------\nLoad Error: {e}"
            )

    @classmethod
    def from_callable(cls, f: U[C, "Func"], env: Env = Env()) -> "Func":
        """
        Generate a func from a variety of possible input data types.
        """
        if isinstance(f, Func):
            # assert not getattr(env,'imports',False)
            return f
        else:
            assert callable(f), f"tried to instantiate Func, but not callable {type(f)}"
            return Func(src=cls.get_source(f), env=env)

    @staticmethod
    def get_source(f: C) -> str:
        """
        Return the source code, even if it's lambda function.
        """
        # Check for built in functions as their source code can not be fetched
        if isbuiltin(f) or (isclass(f) and getattr(f, "__module__", "") == "builtins"):
            raise DBgenInvalidArgument(
                f"Error getting source code for PyBlock. {f} is a built-in function.\n"
                f"Please wrap in lambda like so: `lambda x: {f.__name__}(x)`"
            )

        try:
            source_lines, _ = getsourcelines(f)
        except (OSError, TypeError) as e:
            # functions defined in pdb / REPL / eval / some other way in which
            # source code not clear
            raise ValueError("from_callable: ", f, e)

        # Handle 'def'-ed functions and long lambdas
        src = "".join(source_lines).strip()

        if len(source_lines) > 1 and src[:3] == "def":
            return src

        err = 'Only one "lambda" allowed per line: '
        assert src.count("lambda") == 1, err + src

        src_ = src[src.find("lambda") :]  # start of lambda function

        # Slice off trailing chars until we get a callable function
        while len(src_) > 6:
            try:
                if callable(eval(src_)):
                    return src_
            except (SyntaxError, NameError):
                pass

            src_ = src_[:-1].strip()

        raise ValueError("could not parse lambda: " + src)
