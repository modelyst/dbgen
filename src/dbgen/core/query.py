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

"""
The Query class, as well as Ref (used to indirectly refer to an object in a
query without knowing the exact join path)

Furthermore some Model methods that are highly related to queries are defined.
"""
# External
from typing import Any
from typing import Callable as C
from typing import List as L
from typing import Mapping as M
from typing import Sequence
from typing import Set as S
from typing import Union as U

from hypothesis.strategies import SearchStrategy, builds, dictionaries, lists

from dbgen.core.expr.expr import PK, Expr, true
from dbgen.core.expr.pathattr import PathAttr, expr_attrs

# Internal
from dbgen.core.fromclause import From
from dbgen.core.funclike import Arg
from dbgen.core.misc import ConnectInfo as ConnI
from dbgen.core.schema import Entity, RelTup
from dbgen.utils.lists import flatten, nub
from dbgen.utils.misc import nonempty
from dbgen.utils.sql import select_dict

Fn = C[[Any], str]  # type shortcut

################################################################################
class Query(Expr):
    """
    Specification of a query, which can only be realized in the context of a model

    exprs - things you SELECT for, keys in dict are the aliases of the outputs
    basis - determines how many rows can possibly be in the output
            e.g. ['atom'] means one row per atom
                 ['element','struct'] means one row per (element,struct) pair
    constr - expression which must be evaluated as true in WHERE clause
    aconstr- expression which must be evaluated as true in HAVING clause
             (to do, automatically distinguish constr
             from aconstr by whether or not contains any aggs?)
    option - Objects which may or may not exist (i.e. LEFT JOIN on these)
    opt_attr - Attributes mentioned in query which may be null
            (otherwise NOT NULL constraint added to WHERE)
    """

    def __init__(
        self,
        exprs: M[str, Expr],
        basis: L[U[str, Entity]] = None,
        constr: Expr = None,
        aggcols: Sequence[Expr] = None,
        aconstr: Expr = None,
        option: Sequence[RelTup] = None,
        opt_attr: Sequence[PathAttr] = None,
    ) -> None:
        err = "Expected %s, but got %s (%s)"
        for k, v in exprs.items():
            assert isinstance(k, str), err % ("str", k, type(k))
            assert isinstance(v, Expr), err % ("Expr", v, type(v))

        self.exprs = exprs
        self.aggcols = aggcols or []
        self.constr = constr or true
        self.aconstr = aconstr or None

        if not basis:
            attrobjs = nub([a.obj for a in self.allattr()], str)
            assert len(attrobjs) == 1, f"Cannot guess basis for you {attrobjs}"
            self.basis = attrobjs
        else:
            self.basis = [x if isinstance(x, str) else x.name for x in basis]

        self.option = option or []

        if opt_attr:
            for a in opt_attr:
                if isinstance(a, PK):
                    raise ValueError("You can' have an optional ID attrs currently")
                assert isinstance(a, PathAttr), err % ("PathAttr", a, type(a))
        self.opt_attr = opt_attr or []

    def __str__(self) -> str:
        return "Query<%d exprs>" % (len(self.exprs))

    def __getitem__(self, key: str) -> Arg:
        err = "%s not found in query exprs %s"
        assert key in self.exprs, err % (key, self.exprs)
        return Arg(self.hash, key)

    ####################
    # Abstract methods #
    ####################
    def show(self, f: Fn) -> str:
        """How to represent a Query as a subselect"""
        raise NotImplementedError

    def fields(self) -> L[Expr]:
        """Might be missing a few things here .... """
        return list(self.exprs.values())

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(
            cls,
            exprs=dictionaries(keys=nonempty, values=Expr._strat()),
            basis=lists(nonempty, min_size=1, max_size=2),
            constr=Expr._strat(),
            aggcols=lists(Expr._strat(), max_size=2),
            aconstr=Expr._strat(),
            option=lists(RelTup._strat(), max_size=2),
            opt_attr=lists(PathAttr._strat(), max_size=2),
        )

    ####################
    # Public methods #
    ####################

    def allobj(self) -> L[str]:
        """All object names that are mentioned in query"""

        for a in self.allattr():
            if not hasattr(a, "obj"):
                raise ValueError(f"{a} is missing attr obj")
        return [o.obj for o in self.option] + self.basis + [a.obj for a in self.allattr()]

    def allattr(self) -> S[PathAttr]:
        """All path+attributes that are mentioned in query"""
        agg_x = [expr_attrs(ac) for ac in self.aggcols]  # type: L[L[PathAttr]]
        es = list(self.exprs.values()) + [self.constr, self.aconstr or true]
        out = set(flatten([expr_attrs(expr) for expr in es] + agg_x))
        return out

    def allrels(self) -> S[RelTup]:
        """All relations EXPLICITLY mentioned in the query"""
        out = set(self.option)
        for a in self.allattr():
            out = out | a.allrels()
        return out

    ###################
    # Private methods #
    ###################
    def _make_from(self) -> From:
        """ FROM clause of a query - combining FROMs from all the Paths """
        f = From(self.basis)
        attrs = self.allattr()
        for a in attrs:
            f = f | a.path._from()
        return f

    def showQ(
        self,
        not_deleted: bool = False,
        not_null: bool = True,
        limit: int = None,
    ) -> str:
        """
        Render a query

        To do: HAVING Clause
        """
        # FROM clause
        # ------------
        f = self._make_from()

        # What we select for
        # -------------------
        cols = ",\n\t".join([f'{e} AS "{k}"' for k, e in self.exprs.items()])  # .show(shower)
        cols = "" + cols if cols else ""
        # WHERE and HAVING clauses
        # ---------------------------------
        # Aggregations refered to in WHERE are treated specially
        where = str(self.constr)  # self.show_constr({})
        notdel = "\n\t".join([f'AND NOT COALESCE("{o}".deleted, False)' for o in f.aliases()])
        notnul = "\n\t".join([f"AND {x} IS NOT NULL" for x in self.allattr() if x not in self.opt_attr])
        where_args = [where]
        where_args += [notdel] if not_deleted else []
        where_args += [notnul] if not_null else []

        consts = "WHERE %s" % ("\n\t".join(where_args))

        # HAVING aggregations are treated 'normally'
        if self.aconstr:
            haves = f"\nHAVING {self.aconstr}"
        else:
            haves = ""

        # Group by clause, if anything SELECT'd has an aggregation
        # ---------------------------------------------------------
        def gbhack(x: Expr) -> str:
            """We want to take the CONCAT(PK," ",UID) exprs generated by Entity.id()
            and just replace them with PK"""
            if isinstance(x, PK):
                return str(x.pk)
            else:
                return str(x)

        gb = [gbhack(x) for x in self.aggcols]
        groupby = "\nGROUP BY \n\t%s" % (",\n\t".join(gb)) if gb else ""

        # Compute FROM clause
        # --------------------
        f_str = f.print(self.option)

        # Get the Limit Clause if limit is set
        # ------------------------------------
        limit_str = f"\nLimit {limit}" if limit is not None else ""
        # Put everything together to make query string
        # ----------------------------------------------------
        fmt_args = [cols, f_str, consts, groupby, haves, limit_str]
        output = "SELECT \n\t{}\n{}\n{}{}{}{}".format(*fmt_args)

        return output

    def get_row_count(self, db: ConnI) -> int:
        """
        Queries the database using the connection to get the number of rows this
        query is expected to return.

        Args:
            db (ConnI): ConnectInfo object of the database.

        Raises:
            ValueError: If the query doesn't return an output

        Returns:
            int: number of rows the query will return.
        """
        prepend = """SELECT\n\tCOUNT(*)\nFROM ("""
        append = """\n) AS X"""
        statement = prepend + self.showQ() + append
        query_output = select_dict(db, statement)
        if query_output:
            row_count = query_output[0][0]
            return row_count
        raise ValueError(f"No query output for row_count query,\n{statement}")

    def exec_query(self, db: ConnI) -> L[dict]:
        """Execute a query object in a giving model using a database connection"""
        return select_dict(db, self.showQ())
