# External
from typing import (
    TYPE_CHECKING,
    Set as S,
    List as L,
    Dict as D,
    Tuple as T,
    Union as U,
    Iterator as I,
)
from hashlib import md5
from base64 import b64encode
from networkx import DiGraph  # type: ignore
from collections import defaultdict
from hypothesis.strategies import (
    SearchStrategy,
    builds,
    dictionaries,
    lists,
    sets,
)  # type: ignore

# Internal
if TYPE_CHECKING:
    from dbgen.core.schema import Rel, RelTup, Obj

from dbgen.utils.misc import Base, nonempty
from dbgen.utils.graphs import topsort_with_dict
from dbgen.utils.lists import flatten

################################################################################


class Path(Base):
    """                         (loop 2x)
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

    def __init__(self, end: U[str, "Obj"], fks: list = None) -> None:
        self.end = end if isinstance(end, str) else end.name
        self.fks = fks or []
        err = "expected {} in {} (objs of {})\nall fks: {}"
        if fks and fks[0]:
            if isinstance(fks[0], list):
                for fk in fks[0]:
                    assert self.end in fk[0].objs, err.format(
                        self.end, fk[0].objs, fk[0], self.fks
                    )
            else:
                assert self.end in fks[0].objs, err.format(
                    self.end, fks[0].objs, fks[0], self.fks
                )
        super().__init__()

    def __str__(self) -> str:
        return str(self.join())

    def __repr__(self) -> str:
        return 'JPath("%s", %s)' % (self.end, self.fks)

    def __add__(self, other: "Path") -> "Path":
        """
        Concatenate paths: tail of first must match base of second

        P1 (A --> B), P2 (B --> C) ==> P1 + P2 (A --> C)
        """
        assert other.linear, "Cannot concatenate paths if the second path branches"
        assert (
            self.end == other.base
        ), "Cannot concatenate paths unless head/tail matches"
        return Path(other.end, other.fks + self.fks)

    def __sub__(self, other: "Path") -> "Path":
        """
        Take a path difference to truncate edges from the start of a path
            ... only defined under rare circumstances

        P1 (A --> C), P2 (A --> B) ==> P1 - P2 (B --> C)
        """
        l2 = len(other.fks)
        err = "Cannot take path difference: latter path is not subset of first"
        assert self.fks[-l2:] == other.fks, err
        return Path(self.end, self.fks[:l2])

    @classmethod
    def strat(cls) -> SearchStrategy:
        return builds(cls, end=nonempty)

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
        out = set()
        self.newmethod257(stack, out)
        return out

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
        j = Join(self.end)
        if self.fks:
            nextfk = self.fks[0]
            if isinstance(nextfk, list):
                for nex in nextfk:
                    p = Path(nex[0].other(self.end), nex[1:])
                    j.add(p.join(), nex[0])
            else:
                nextab = nextfk.other(self.end)
                try:
                    nextpath = Path(nextab, self.fks[1:])
                except:
                    import pdb

                    pdb.set_trace()
                    assert False
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

    def __init__(self, obj: str, conds: L[T["Join", S["Rel"]]] = None) -> None:
        assert isinstance(obj, str)
        self.obj = obj
        self.conds = conds or []
        super().__init__()

    def __str__(self) -> str:
        return self.alias

    def __repr__(self) -> str:
        return "JOIN %s (%s)" % (self.obj, ",".join(map(str, self.conds)))

    def __lt__(self, other: "Join") -> bool:
        return str(self) < str(other)

    @property
    def conddict(self) -> D["Join", S["Rel"]]:
        return dict(self.conds)

    @classmethod
    def strat(cls) -> SearchStrategy:
        return builds(cls, obj=nonempty, conds=dictionaries(Join.strat(), Rel.strat()))

    ### Public Methods ###
    def add(self, j: "Join", e: "Rel") -> None:
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
            s += "[(%s)#(%s)]" % (fkstr, j.alias)

        data = s + self.obj
        m = md5(data.encode("ascii"))
        out = b64encode(m.digest()).decode("ascii")[:3]
        return self.obj + "(%s)" % out

    def print(self, optional: L["RelTup"] = None) -> str:
        """Render JOIN statement in FROM clause"""
        conds = [self._cond(j, e) for j, e in self.conds]  # conditions to join on
        opts = optional or []
        if not bool(conds):
            jointype = " CROSS "
        else:
            left = True
            # Assume a left join. if any FKs in current edge are NOT in "optional", then set to Inner join
            for e in self.conddict.values():
                for fk in e:  # type: ignore
                    if fk.tup() not in opts:
                        left = False
                        break
            jointype = " LEFT " if left else " INNER "
        on = (
            " ON " + "\n\t\t\tAND ".join(conds) if conds else ""
        )  # Possibly do not join on any condition
        args = [jointype, self.obj, self.alias, on]
        return '\n\t{}JOIN {} AS "{}" {}'.format(*args)

    ## Private Methods ###

    def _cond(self, j: "Join", rels: S["Rel"]) -> str:
        """Assume the alias defined by the arg's Join has already been defined
            in the FROM statement. Write a SQL JOIN condition that will be used
            to define the current Join object's alias

            NEED TO MODIFY EVERYTHING SO THAT WE USE <object>.id, not <object>.name + '_id'
            """

        conds = []  # type: L[str]
        for fk in rels:
            o = fk.other(self.obj)
            forward = (
                o == fk.o1
            )  # Rel in forward direction. Self.obj is the 'old table'
            aliases = [j.alias, self.alias]
            cols = [fk.name, self.obj + "_id"] if forward else [o + "_id", fk.name]
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

    @classmethod
    def strat(cls) -> SearchStrategy:
        return builds(
            cls,
            basis=lists(nonempty, min_size=1, max_size=2),
            conds=sets(Join.strat(), min_size=1, max_size=2),
        )

    def print(self, optional: L["RelTup"] = None) -> str:
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
            [
                (col + ' AS "{0}" ').format(a, j.obj)
                for a, j in sorted(zip(self.aliases(), self.joins))
            ]
        )

