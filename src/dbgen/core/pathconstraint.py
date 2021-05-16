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

from typing import TYPE_CHECKING
from typing import Iterator as Iter
from typing import List as L
from typing import Union as U

from infinite import product
from networkx import DiGraph

from dbgen.core.fromclause import Path

# Internal
from dbgen.utils.misc import Base

if TYPE_CHECKING:
    from dbgen.core.schema import Entity, Rel, RelTup
    from dbgen.core.schemaclass import Schema

    Schema, Rel, RelTup, Entity
##############################################################################


class Constraint(Base):
    """
    A way of specifying HOW one arrives at a certain table through JOINing on
    relations. Under normal circumstances, we constrain possible JOIN paths by
    giving a sequence of relations that must be traversed in a certain order.

    This alone would only allow for linear (nonbranching) paths.

    It is possible that a table should be arrived at via multiple
    distinct paths, in which case "branch" should be used.
    """

    def __init__(
        self,
        tab: U[str, "Entity"],
        reqs: L[U["Rel", "RelTup"]] = None,  # constraints on final 'linear' sequence
        xclude: L[U["Rel", "RelTup"]] = None,  # Never use these
        backtrack: bool = False,  # allow the
        branch: L["Constraint"] = None,  # any branching prior to final sequence
    ) -> None:
        self.tab = tab if isinstance(tab, str) else tab.name
        self.reqs = reqs or []
        self.xclude = set(xclude or [])
        self.backtrack = backtrack
        self.branch = set(branch) if branch else set()

        if branch:
            err = "All branches must start from same table %s"
            tabs = {b.tab for b in self.branch}
            assert len(tabs) == 1, err % tabs
        super().__init__()

    def __str__(self) -> str:
        return f"Constraint<{self.tab}>"

    @property
    def branch_obj(self) -> str:
        """Get the branching object, if it exists"""
        return next(iter(self.branch)).tab

    def find(
        self,
        m: "Schema",
        basis: U[str, "Entity", L[U[str, "Entity"]]],
        links: L[U["Rel", "RelTup"]] = None,
        quit: bool = True,
    ) -> None:
        """
        User interface for getting paths
        """
        msg = """Searching for paths from %s to %s\n""" + "%s\n\t" * 4

        # Compute informatino graph in light of the 'links'
        ig = m._info_graph(links or []).reverse()

        # Process basis data
        if not isinstance(basis, list):
            basis_ = [basis]
        else:
            basis_ = basis
        base = [x if isinstance(x, str) else x.name for x in basis_]
        bstr = ",".join(base)

        self._update_reltup(m)

        # underline important things
        l1, l2, l3, l4 = 25, len(bstr), 4, len(self.tab)
        under = " " * l1 + "#" * l2 + " " * l3 + "#" * l4
        constrstr = f"(constraints {self.reqs})" if self.reqs else ""
        branchstr = f"(branch {self.branch})" if self.branch else ""
        linkstr = f"(bypass chickenfeet for {links})" if links else ""

        print(msg % (bstr, self.tab, under, constrstr, branchstr, linkstr))
        for path in self.find_path(ig, base):
            print(repr(path))
            print(path._from().print())
            if input("\nContinue?\n\ty/n -> ").lower() not in ["y", "yes"]:
                break

        if quit:
            exit()

    def find_path(self, schema: DiGraph, basis: L[str]) -> Iter[Path]:
        """Work backwards to find a path to the target that satisfies constraints"""
        # Initialize with empty FK graph, all requirements present
        stack = [(Path(self.tab), list(self.reqs))]
        # Explore possible states in BFS
        while stack:
            # Get current state
            path, reqs = stack.pop(0)
            # print('\n\tnew iteration',path,reqs)

            # Enter the branching logic if we are at branchobj
            if not reqs and self.branch and path.base == self.branch_obj:
                branchiters = [c.find_path(schema, basis) for c in self.branch]
                for branches in product(*branchiters):
                    G = path.copy()
                    for branch in branches:
                        G = G.add_branch(branch)
                    yield G
                print("No more paths")
                return

            if not reqs and path.base in basis:
                yield path  # We're done!
            else:
                # Possible FKs to join on
                for edge in schema[path.base]:
                    seen = set() if self.backtrack else path.all_rels()
                    fks = sorted(set(schema[path.base][edge]["fks"]) - self.xclude - seen)
                    for fk in fks:

                        if reqs:
                            rs = reqs[:-1] if reqs[-1] == fk else list(reqs)
                        else:
                            rs = []
                        stack.append((path.add(fk), rs))

        print("No more paths")

    def _update_reltup(self, m: "Schema") -> "None":
        self.reqs = [m.get_rel(r) for r in self.reqs]
        self.branch = {b.__update_reltup(m) for b in self.branch}

    def __update_reltup(self, m: "Schema") -> "Constraint":
        """Version of _update_reltup that returns a copy"""
        c = self.copy()
        c._update_reltup(m)
        return c
