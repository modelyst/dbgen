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

from base64 import b64encode
from hashlib import md5

# External
from typing import TYPE_CHECKING
from typing import Dict as D
from typing import List as L
from typing import Sequence
from typing import Set as S
from typing import Tuple as T
from typing import Union as U

from dbgen.utils.lists import flatten
from dbgen.utils.misc import Base

# Internal
if TYPE_CHECKING:
    from dbgen.core.schema import Entity, Rel, RelTup, SuperRel

    Rel, SuperRel, RelTup, Entity
################################################################################


class Path(Base):
    """
    (loop 2x)
                 ->           ^|
            -> C -> D         |v
    A -> B            -> E -> F -> G
              -> H

    Would be represented like so:
    Path('G',[fg,ff,ff,ef,[[de,[[cd1,bc,ab],
                                [cd2,bc,ab]],
                           [he,bh,ab]
                          ]])
    """

    def __init__(self, end: U[str, "Entity"], fks: list = None, name: str = None) -> None:
        self.end = end if isinstance(end, str) else end.name
        self.fks = fks or []
        self.name = name
        err = "expected {} in {} (objs of {})\nall fks: {}"

        if fks and fks[0]:
            if isinstance(fks[0], list):
                for fk in fks[0]:
                    assert self.end in fk[0].objs, err.format(self.end, fk[0].objs, fk[0], self.fks)
            else:
                assert self.end in fks[0].objs, err.format(self.end, fks[0].objs, fks[0], self.fks)
        super().__init__()

    def __str__(self) -> str:
        return str(self.join())

    def __repr__(self) -> str:
        return f'JPath("{self.end}", {self.fks})'

    def __add__(self, other: "Path") -> "Path":
        """
        Concatenate paths: tail of first must match base of second

        P1 (A --> B), P2 (B --> C) ==> P1 + P2 (A --> C)
        """
        assert other.linear, "Cannot concatenate paths if the second path branches"
        assert self.end == other.base, "Cannot concatenate paths unless head/tail matches"
        assert self.name == other.name, "Cannot concatenate paths with different names"
        return Path(other.end, other.fks + self.fks, name=self.name)

    def __sub__(self, other: "Path") -> "Path":
        """
        Take a path difference to truncate edges from the start of a path
            ... only defined under rare circumstances

        P1 (A --> C), P2 (A --> B) ==> P1 - P2 (B --> C)
        """
        l2 = len(other.fks)
        err = "Cannot take path difference: latter path is not subset of first"
        assert self.fks[-l2:] == other.fks, err
        assert self.name == other.name, "Cannot take path difference with different names"
        return Path(self.end, self.fks[:l2], name=self.name)

    @property
    def linear(self) -> bool:
        return all([not isinstance(fk, list) for fk in self.fks])

    @property
    def base(self) -> str:
        """The start of the current join path, defined iff it is linear"""
        assert self.linear
        curr = self.end
        for fk in self.fks:
            curr = fk.other(curr)
        return curr

    def all_rels(self) -> S["Rel"]:
        stack = self.fks
        out: S["SuperRel"] = set()
        self.newmethod257(stack, out)
        return {o.to_rel() for o in out}

    def newmethod257(self, stack, out):
        while stack:
            curr = stack.pop()

            if isinstance(curr, list):
                assert not stack
                stack = flatten(curr)
            else:
                out.add(curr)

    def add(self, r: "Rel") -> "Path":
        assert self.base in r.objs
        return Path(self.end, self.fks + [r])

    def add_branch(self, p: "Path") -> "Path":
        c = self.copy()  # only work with a deepcopy

        # Check if this is the first branch we're adding
        if not c.fks or not isinstance(c.fks[-1], list):
            c.fks.append([p.fks])
        else:
            c.fks[-1].append(p.fks)
        return c

    def join(self) -> "Join":
        """Get top-level join that is implied by this path."""
        j = Join(self.end, name=self.name)
        if self.fks:
            nextfk = self.fks[0]
            if isinstance(nextfk, list):
                for nex in nextfk:
                    p = Path(nex[0].other(self.end), nex[1:], name=self.name)
                    j.add(p.join(), nex[0])
            else:
                nextab = nextfk.other(self.end)
                nextpath = Path(nextab, self.fks[1:], name=self.name)
                j.add(nextpath.join(), nextfk)
        return j

    def alljoin(self) -> S["Join"]:
        stack = [self.join()]
        joins = set()  # type: S['Join']
        while stack:
            curr = stack.pop()
            if curr not in joins:
                joins.add(curr)
                stack.extend(list(curr.conddict.keys()))
        return joins

    def _from(self) -> "From":
        return From(joins=self.alljoin())


class Join(Base):
    """
    Constructed from a Path object

    Represents a table plus a unique set of JOINs that led to that table
        (accounting for multiple linear paths)
    """

    def __init__(self, obj: str, conds: L[T["Join", S["SuperRel"]]] = None, name: str = None) -> None:
        assert isinstance(obj, str)
        self.obj = obj
        self.conds = conds or []
        self.name = name
        super().__init__()

    def __str__(self) -> str:
        return self.alias

    def __repr__(self) -> str:
        return f"JOIN {self.obj} ({','.join(map(str, self.conds))})"

    def __lt__(self, other: "Join") -> bool:
        return str(self) < str(other)

    @property
    def conddict(self) -> D["Join", S["SuperRel"]]:
        return dict(self.conds)

    # Public Methods
    def add(self, j: "Join", e: "SuperRel") -> None:
        if j in self.conddict:
            d = dict(self.conddict)
            d[j].add(e)
            self.conds = list(d.items())  # overwrite
        else:
            self.conds.append((j, {e}))

    @property
    def alias(self) -> str:
        """How to uniquely refer to this PATH through the schema"""

        if not self.conds:
            return self.obj

        s = ""
        for j, fks in sorted(self.conds):
            fkstr = "|".join([fk.print() for fk in sorted(fks)])
            s += f"[({fkstr})#({j.alias})]"

        data = s + self.obj
        m = md5(data.encode("ascii"))
        out = b64encode(m.digest()).decode("ascii")[:3]
        out_name = f"{self.name}-" if self.name is not None else ""
        return self.obj + f"({out_name}{out})"

    def print(self, optional: L["RelTup"] = None) -> str:
        """Render JOIN statement in FROM clause"""
        conds = [self._cond(j, e) for j, e in self.conds]  # conditions to join on
        opts = optional or []
        if not bool(conds):
            jointype = " CROSS "
        else:
            left = True
            # Assume a left join. if any FKs in current edge are NOT in
            # "optional", then set to Inner join
            for e in self.conddict.values():
                for fk in e:  # type: ignore
                    if fk.tup() not in opts:
                        left = False
                        break
            jointype = " LEFT " if left else " INNER "
        on = (
            " ON \n\t\t\t" + "\n\t\t\tAND ".join(conds) if conds else ""
        )  # Possibly do not join on any condition
        args = [jointype, self.obj, self.alias, on]
        return '\n\t{}JOIN {} AS "{}" {}'.format(*args)

    # Private Methods

    def _cond(self, j: "Join", rels: S["SuperRel"]) -> str:
        """Assume the alias defined by the arg's Join has already been defined
        in the FROM statement. Write a SQL JOIN condition that will be used
        to define the current Join object's alias

        NEED TO MODIFY EVERYTHING SO THAT WE USE <object>.id, not <object>.name + '_id'
        """

        conds = []  # type: L[str]
        for fk in rels:
            o = fk.other(self.obj)
            forward = o == fk.source  # Rel in forward direction. Self.obj is the 'old table'
            aliases = [j.alias, self.alias]
            cols = [fk.name, fk.target_id_str] if forward else [fk.target_id_str, fk.name]
            args = [aliases[0], cols[0], aliases[1], cols[1]]
            new = ' "{}"."{}" = "{}"."{}" '.format(*args)
            conds.append(new)
        return "\n\t\tAND ".join(conds)


class From(Base):
    """
    Class used to help construct a WHERE clause.
    """

    def __init__(self, basis: L[str] = None, joins: S[Join] = None) -> None:
        self.joins = {Join(b) for b in basis or []} | (joins or set())
        self.basis = {j.obj for j in self.joins if not j.conds}
        super().__init__()

    # @property --- IS THIS GOING TO BREAK STUFF NOW THAT IT'S A FIELD
    # def basis(self) -> S[str]: return {j.obj for j in self.joins if not j.conds}

    # Public methods #
    def __str__(self) -> str:
        return "From(basis=%s,%d joins)" % (self.basis, len(self.joins))

    def __or__(self, f: "From") -> "From":
        return From(joins=self.joins | f.joins)

    def print(self, optional: Sequence["RelTup"] = None) -> str:
        from dbgen.utils.graphs import DiGraph, topsort_with_dict

        d = {j.alias: j for j in self.joins}
        G = DiGraph()
        G.add_nodes_from(d.keys())
        for j in self.joins:
            for j2 in j.conddict.keys():
                G.add_edge(j.alias, j2.alias)
        sort = list(reversed(topsort_with_dict(G, d)))
        start = sort[0].obj
        assert start in self.basis
        return "FROM " + start + "".join([j.print(optional) for j in sort[1:]])

    def aliases(self) -> L[str]:
        return [j.alias for j in self.joins]

    def pks(self, agg: bool = False) -> str:
        col = 'MAX("{0}"."{1}_id")' if agg else '"{0}"."{1}_id"'
        return ",\n\t".join(
            [(col + ' AS "{0}" ').format(a, j.obj) for a, j in sorted(zip(self.aliases(), self.joins))]
        )
