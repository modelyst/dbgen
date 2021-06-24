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

import logging

# Extenral
from abc import ABCMeta, abstractmethod
from traceback import format_exc
from typing import Any
from typing import Callable as C
from typing import Dict as D
from typing import List as L
from typing import Sequence
from typing import Tuple as T
from typing import Union as U

from hypothesis.strategies import SearchStrategy, builds, just, one_of

# Internal
from dbgen.core.func import Env, Func
from dbgen.core.misc import ConnectInfo as Conn
from dbgen.utils.exceptions import (
    DBgenExternalError,
    DBgenInvalidArgument,
    DBgenMissingInfo,
    DBgenSkipException,
)
from dbgen.utils.lists import is_iterable
from dbgen.utils.log import capture_stdout
from dbgen.utils.misc import Base, anystrat
from dbgen.utils.sql import mkInsCmd, sqlexecute

"""
The "T" of ETL: things that are like functions.

A generalization of a function is a computational graph (where nodes are functions)

These nodes (PyBlocks) have Args which refer to other PyBlocks (or a query)
"""
####################################################################################
class ArgLike(Base, metaclass=ABCMeta):
    @abstractmethod
    def arg_get(self, dic: dict) -> Any:
        raise NotImplementedError

    @abstractmethod
    def make_src(self, meta: bool = False) -> str:
        raise NotImplementedError

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return one_of(Arg._strat(), Const._strat())


class Arg(ArgLike):
    """
    How a function refers to a namespace
    """

    def __init__(self, key: str, name: str) -> None:
        self.key = str(key)
        self.name = name.lower()

    def __str__(self) -> str:
        return f"Arg({str(self.key)[:4]}...,{self.name})"

    # Public methods #
    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)

    def arg_get(self, dic: dict) -> Any:
        """
        Common interface for Const and Arg to get values out of namespace
        """
        try:
            val = dic[self.key][self.name]
            return val
        except KeyError:
            if self.key not in dic:
                raise DBgenMissingInfo(
                    f"could not find hash, looking for {self.name} at this hash {self.key}"
                )
            else:
                err = "could not find '%s' in %s "
                raise DBgenMissingInfo(err % (self.name, list(dic[self.key].keys())))

    def add(self, cxn: Conn, act: int, block: int) -> None:
        q = mkInsCmd("_arg", ["gen_id", "block_id", "hashkey", "name"])
        sqlexecute(cxn, q, [act, block, self.key, self.name])

    def make_src(self, meta: bool = False) -> str:
        key = repr(self.key) if isinstance(self.key, str) else self.key
        if meta:
            return f"Arg({key},'{self.name}')"
        else:
            return f'namespace[{key}]["{self.name}"]'


class Const(ArgLike):
    def __init__(self, val: Any) -> None:
        if callable(val):
            val = Func.from_callable(val)
        self.val = val

    def __str__(self) -> str:
        return f"Const<{self.val}>"

    def arg_get(self, _: dict) -> Any:
        return self.val

    def make_src(self, meta: bool = False) -> str:
        if meta:
            return "Const(%s)" + repr(self.val)
        else:
            return repr(self.val)

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls, val=anystrat)


class PyBlock(Base):
    """
    A computational block that executes Python code
    """

    def __init__(
        self,
        func: U[C, Func],
        env: Env = Env(),
        args: Sequence[ArgLike] = [],
        outnames: L[str] = None,
        tests: L[T[tuple, Any]] = [],
    ) -> None:

        # Store fields
        self.func = Func.from_callable(func, env=env)
        self.env = env
        self.args = args
        self.outnames = [o.lower() for o in outnames or ["out"]]
        self.tests = tests
        # Validate inputs for errors
        self.validate_inputs()

        super().__init__()

    @classmethod
    def _strat(cls) -> SearchStrategy:
        func = lambda x: x + 1
        return just(
            cls(
                func=func,
                args=[Const(1)],
                outnames=["x"],
            )
        )

    def __str__(self) -> str:
        return "PyBlock<%d args>" % (len(self.args))

    def __lt__(self, other: "PyBlock") -> bool:
        return self.hash < other.hash

    def __call__(self, curr_dict: D[str, D[str, Any]], log_level: int = logging.INFO) -> D[str, Any]:
        """
        Take a TempFunc's function and wrap it such that it can accept a namespace
            dictionary. The information about how the function interacts with the
            namespace is contained in the TempFunc's args.
        """
        try:
            inputvars = [arg.arg_get(curr_dict) for arg in self.args]
        except (TypeError, IndexError) as e:
            print(e)
            raise ValueError()
        except AttributeError:
            invalid_args = [getattr(arg, "arg_get", None) is None for arg in self.args]
            missing_args = filter(lambda x: invalid_args[x], range(len(invalid_args)))
            raise DBgenMissingInfo(
                f"Argument(s) {' ,'.join(map(str,missing_args))} to {self.func.name} don't have arg_get attribute:\n Did you forget to wrap a Const around a PyBlock Arguement?"
            )

        try:
            wrapped = capture_stdout(self.func, level=log_level)
            output = wrapped(*inputvars)
            if isinstance(output, tuple):
                l1, l2 = len(output), len(self.outnames)
                assert l1 == l2, "Expected %d outputs from %s, got %d" % (
                    l2,
                    self.func.name,
                    l1,
                )
                return {o: val for val, o in zip(output, self.outnames)}
            else:
                assert len(self.outnames) == 1
                return {self.outnames[0]: output}
        except DBgenSkipException:
            raise
        except Exception:
            msg = f"\tApplying func {self.func.name} in tempfunc:\n\t"
            raise DBgenExternalError(msg + format_exc())

    def __getitem__(self, key: str) -> Arg:
        err = "%s not found in %s"
        assert key.lower() in self.outnames, err % (key, self.outnames)
        return Arg(self.hash, key.lower())

    def _constargs(self) -> L[Func]:
        """all callable constant args"""
        return [c.val for c in self.args if isinstance(c, Const) and isinstance(c.val, Func)]

    def validate_inputs(self) -> None:
        # Check for iterability
        if not is_iterable(self.outnames):
            raise DBgenInvalidArgument(
                f"Func Name: {self.func.name}.\nOutnames is not iterable. Please wrap in singleton list if single argument"
            )
        elif not is_iterable(self.args):
            raise DBgenInvalidArgument(
                f"Func Name: {self.func.name}.\nArgs is not iterable. Please wrap in singleton list if single argument"
            )
        # Check for duplicate outnames
        if self.outnames:
            dups = [o for o in self.outnames if self.outnames.count(o) > 1]
            if dups:
                err = f"No duplicates in outnames for func {self.func.name}: {dups}"
                raise DBgenInvalidArgument(err)
        invalid_args = [i for i, arg in enumerate(self.args) if not isinstance(arg, ArgLike)]
        if invalid_args:
            raise DBgenMissingInfo(
                f"Argument(s) {' ,'.join(map(str,invalid_args))} to {self.func.name} are not ArgLike:\n Did you forget to wrap a Const around a PyBlock Arguement?"
            )

    def add(self, cxn: Conn, a_id: int, b_id: int) -> None:
        """Add to metaDB"""
        cols = ["gen_id", "py_block_id", "func_id", "outnames"]
        f_id = self.func.add(cxn)
        q = mkInsCmd("_py_block", cols)
        sqlexecute(cxn, q, [a_id, b_id, f_id, ",".join(self.outnames)])

    def test(self) -> None:
        """Throw error if any tests are failed"""
        for args, target in self.tests:
            assert self.func(*args) == target

    def make_src(self) -> str:
        # load the pyblock template
        from dbgen.templates import jinja_env

        pb_template = jinja_env.get_template("pyblock.py.jinja")

        # Prepare the template args
        args = [a.make_src() for a in self.args]
        name = self.func.name.replace("<lambda>", "func")
        src = ("func=" if "<lambda>" in self.func.name else "") + self.func.src.replace("\t", "   ")

        # Set the template args
        template_kwargs = dict(src=src, outnames=self.outnames, args=args, name=name)
        rendered_template = pb_template.render(**template_kwargs)
        tabified_template = rendered_template.replace("\n", "\n    ")
        return tabified_template
