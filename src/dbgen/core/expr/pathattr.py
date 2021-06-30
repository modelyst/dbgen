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

from typing import TYPE_CHECKING, Any
from typing import Callable as C
from typing import List as L
from typing import Optional as Opt
from typing import Set as S

if TYPE_CHECKING:
    from hypothesis.strategies import SearchStrategy

    SearchStrategy
from dbgen.core.expr.expr import Expr
from dbgen.core.fromclause import Path
from dbgen.core.schema import AttrTup, RelTup
from dbgen.utils.lists import flatten

Fn = C[[Any], str]  # type shortcut

########################
class PathAttr(Expr):
    def __init__(self, path: Opt[Path], attr: AttrTup) -> None:
        assert attr
        self.path = path or Path(attr.obj)
        self.attr = attr

    def __str__(self) -> str:
        # If attribute name is *
        if self.attr.name == "*":
            return f'"{self.path}".{self.attr.name}'
        return f'"{self.path}"."{self.attr.name}"'

    def __repr__(self) -> str:
        return f"PathAttr<{self.path}.{self.attr.name}>"

    @classmethod
    def _strat(cls) -> "SearchStrategy":
        from hypothesis.strategies import builds, from_type, none

        return builds(cls, path=none(), attr=from_type(AttrTup))

    ####################
    # Abstract methods #
    ####################
    def attrs(self) -> L["PathAttr"]:
        return [self]

    def fields(self) -> list:
        """
        List of immediate substructures of the expression (not recursive)
        """
        return []

    def show(self, f: Fn) -> str:
        """Apply function recursively to fields."""
        return f(self)

    @property
    def name(self) -> str:
        return self.attr.name

    @property
    def obj(self) -> str:
        return self.attr.obj

    def allrels(self) -> S[RelTup]:
        stack = list(self.path.fks)
        rels = set()
        while stack:
            curr = stack.pop(0)
            if not isinstance(curr, list):
                rels.add(curr.tup())
            else:
                assert not stack  # only the last element should be a list
                stack = flatten(curr)
        return rels


################################################################################
def expr_attrs(expr: Expr) -> L["PathAttr"]:
    """Recursively search for any Path (Expr) mentions in the Expr."""
    out = [expr] if isinstance(expr, PathAttr) else []  # type: L['PathAttr']
    if hasattr(expr, "fields"):
        for field in expr.fields():
            out.extend(expr_attrs(field))

    return out
