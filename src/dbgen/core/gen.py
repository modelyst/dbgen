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

from collections import defaultdict
from typing import TYPE_CHECKING, Any
from typing import Dict as D
from typing import List as L
from typing import Optional as Opt
from typing import Set as S
from typing import Tuple as T

import hypothesis.strategies as st
from hypothesis.strategies import SearchStrategy, builds, lists, text

from dbgen.core.func import Env, Func
from dbgen.core.funclike import Arg, PyBlock
from dbgen.core.load import Load
from dbgen.core.misc import Dep
from dbgen.core.query import Query
from dbgen.core.schema import Entity
from dbgen.utils.exceptions import DBgenSkipException
from dbgen.utils.lists import concat_map
from dbgen.utils.misc import Base
from dbgen.utils.sql import Connection as Conn
from dbgen.utils.sql import DictCursor, mkInsCmd, mkSelectCmd, mkUpdateCmd, sqlexecute, sqlselect
from dbgen.utils.str_utils import hash_

# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model

    Model
"""
Defines a Generator, as well as a Model method that is directly related
"""
################################################################################


class Generator(Base):
    """
    Generator: populates database with data
    One of the two component objects of a DBgen model
    """

    def __init__(
        self,
        name: str,
        desc: str = None,
        query: Query = None,
        transforms: L[PyBlock] = None,
        loads: L[Load] = None,
        tags: L[str] = None,
        batch_size: int = None,
        additional_deps: Dep = None,
    ) -> None:
        """
        Initializes generator object.

        [Query][dbgen.core.query.Query]

        Args:
            name (str): name of the generator
            desc (str, optional): Description of what the generator does. Defaults to None.
            query (Query, optional): [Query][dbgen.core.query.Query] object. Defaults to None.
            transforms (L[PyBlock], optional): [description]. Defaults to None.
            loads (L[Load], optional): [description]. Defaults to None.
            tags (L[str], optional): [description]. Defaults to None.
            env (Env, optional): [description]. Defaults to None.
            batch_size (int, optional): [description]. Defaults to None.
        """
        # assert loads, 'Cannot have generator which does nothing'
        assert name
        self.name = name.lower()
        self.desc = desc or "<no description>"
        self.query = query
        self.transforms = self._order_transforms(transforms or [], query)
        self.loads = loads or []
        self.tags = [t.lower() for t in tags or []]
        self.batch_size: Opt[int] = batch_size
        self.additional_deps = additional_deps
        super().__init__()

    def __str__(self) -> str:
        return f"Gen<{self.name}>"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        """A hypothesis strategy for generating random examples."""
        limited_text = text(min_size=1, max_size=2)
        return builds(
            cls,
            name=limited_text,
            desc=limited_text,
            query=st.one_of([st.none(), Query._strat()]),
            transforms=lists(PyBlock._strat(), max_size=2),
            loads=lists(Load._strat(), min_size=0, max_size=2),
            tags=lists(limited_text, max_size=3),
        )

    ##################
    # Public Methods #
    ##################

    def update_status(self, conn: Conn, run_id: int, status: str, err: str = "") -> None:
        """Update this gens status in the meta-database, also set error if provided"""
        cols = ["status", "error"] if err else ["status"]
        binds = [status, err, run_id, self.name] if err else [status, run_id, self.name]
        q = mkUpdateCmd("gens", cols, ["run", "name"])
        sqlexecute(conn, q, binds)

    def get_id(self, c: Conn) -> L[tuple]:  # THIS IS OBSOLETE BC HASH IS ID?
        """ Assuming we've inserted already """
        check = self.hash
        get_a = mkSelectCmd("gen", ["gen_id"], ["gen_id"])
        return sqlselect(c, get_a, [check])

    def hasher(self, x: Any) -> str:
        """Unique hash function to this Generator"""
        return hash_(str(self.hash) + str(x))

    def dep(self, universe: D[str, Entity]) -> Dep:
        """
        Determine the tabs/cols that are both inputs and outputs to the Gen

        Args:
            universe (D[str, Entity]): Mapping of object name to DBgen

        Returns:
            Dep
        """
        # Analyze allattr and allobj to get query dependencies
        if self.query:
            tabdeps = self.query.allobj()
            coldeps = [f"{a.obj}.{a.name}" for a in self.query.allattr()]
            for r in self.query.allrels():
                coldeps.append(r.obj + "." + r.rel)
        else:
            tabdeps, coldeps = [], []

        # Analyze loads to see what new cols and tabs are yielded
        newtabs: L[str] = []
        newcols: L[str] = []

        for a in self.loads:
            tabdeps.extend(a.tabdeps(universe))
            newtabs.extend(a.newtabs(universe))
            newcols.extend(a.newcols(universe))

        # Allow for unethical hacks
        for t in self.tags:
            if t[:4] == "dep ":
                coldeps.append(t[4:])

        implicit_deps = Dep(tabdeps, coldeps, newtabs, newcols)
        # check if we have additional explicit dependencies
        if self.additional_deps:
            total_deps = Dep.merge([implicit_deps, self.additional_deps])
        else:
            total_deps = implicit_deps
        return total_deps

    def add(self, cxn: "Conn") -> int:
        """
        Add the Generator to the metaDB which stores info about a model (if
        it's not already in there) and return the ID
        """
        a_id = self.get_id(cxn)
        if a_id:
            return a_id[0][0]
        else:
            cmd = mkInsCmd("gen", ["gen_id", "name", "description", "gen_json"])
            sqlexecute(cxn, cmd, [self.hash, self.name, self.desc, self.toJSON()])
            aid = self.get_id(cxn)
            return aid[0][0]

    def rename_object(self, o: Entity, n: str) -> "Generator":
        """Change all references to an object to account for name change"""
        g = self.copy()
        if g.query:
            g.query.basis = [n if b == o.name else b for b in g.query.basis]
        for i, a in enumerate(g.loads):
            g.loads[i] = a.rename_object(o, n)
        return g

    def purge(self, conn: Conn, mconn: Conn, universe: D[str, Entity]) -> None:
        """
        If a generator is purged, then any
        tables it populates will be truncated. Any columns it populates will be set all
        to NULL
        """
        d = self.dep(universe)
        tabs, cols = d.tabs_yielded, d.cols_yielded
        for t in tabs:
            sqlexecute(conn, f"TRUNCATE {t} CASCADE")

        for t, c in map(lambda x: x.split("."), cols):
            sqlexecute(mconn, f"UPDATE {t} SET {c} = NULL")

        gids = sqlselect(mconn, "SELECT gen_id FROM gen WHERE name = %s", [self.name])
        for gid in gids:
            sqlexecute(mconn, "DELETE FROM repeats WHERE gen_id = %s", [gid])

    def test(
        self,
        universe,
        input_rows: L[D[str, Any]],
        rename_dict: bool = True,
        verbose: bool = False,
    ) -> T[L[dict], L[D[str, L[D[str, Any]]]]]:
        # Apply the
        output_dicts = []
        for row in input_rows:
            result_dict = {self.query.hash: row} if self.query else {}
            try:
                if verbose:
                    from tqdm import tqdm

                    with tqdm(total=len(self.transforms)) as tq:
                        for pb in self.transforms:
                            tq.set_description(pb.func.name)
                            result_dict[pb.hash] = pb(result_dict)
                            tq.update()
                else:
                    for pb in self.transforms:
                        result_dict[pb.hash] = pb(result_dict)
            except DBgenSkipException as exc:
                result_dict["DBGEN_ERROR"] = exc.msg
                output_dicts.append(result_dict)
                continue
            # Replace pyblock hashes with function names if flag is True
            lambda_count = 0
            func_name_dict = {}
            name_count: D[str, int] = defaultdict(int)
            for pb in self.transforms:
                name = pb.func.name
                if pb.func.is_lam:
                    func_name_dict[pb.hash] = f"lambda{lambda_count}->{pb.outnames}"
                    lambda_count += 1
                else:
                    # Need to account for multiple pyblocks using same function
                    if name_count[name] > 0:
                        func_name_dict[pb.hash] = "_".join([name, str(name_count[name])])
                    else:
                        func_name_dict[pb.hash] = name
                    name_count[name] += 1

            output_dicts.append(result_dict)

        load_dicts: D[str, list] = defaultdict(list)
        for i, a in enumerate(self.loads):
            output_dict = a.test(universe=universe, rows=output_dicts)
            for table_name, rows in output_dict.items():
                load_dicts[table_name].extend(rows)

        # Rename PyBlocks
        if rename_dict:
            for i, row in enumerate(output_dicts):
                output_dicts[i] = {func_name_dict.get(key, "query"): val for key, val in row.items()}

        return output_dicts, [load_dicts]

    def test_with_db(
        self,
        universe,
        db: Conn = None,
        limit: int = 5,
        rename_dict: bool = True,
        interact: bool = False,
        input_rows: L[dict] = [],
    ) -> T[L[D[str, dict]], L[D[str, L[dict]]]]:
        assert limit <= 200, "Don't allow for more than 200 rows with test with db"
        assert (
            db is not None or input_rows
        ) or self.query is None, "Need to provide a db connection if generator has a query"

        if db is not None and self.query is not None:
            cursor = db.connect(auto_commit=False).cursor(f"test-{self.name}", cursor_factory=DictCursor)
            # If there is a query get the row count and execute it
            query_str = self.query.showQ(limit=limit)
            print("Executing Query...")
            cursor.execute(query_str)
            input_rows.extend(cursor.fetchall())
            print("Fetching Rows...")
            print("Closing Connection...")
            cursor.close()

        if interact:
            from dbgen.utils.interact import interact_gen

            return interact_gen(universe, self, input_rows)
        else:
            if self.query is None and len(input_rows) == 0:
                input_rows = [{}]

            return self.test(universe, input_rows, rename_dict)

    @property
    def env(self) -> Env:
        self._env = Env()
        for pyblock in self.transforms:
            self._env += pyblock.func.env
        return self._env

    ##################
    # Private Methods #
    ##################

    def _constargs(self) -> L[Func]:
        return concat_map(lambda y: y._constargs(), self.transforms)

    @staticmethod
    def _order_transforms(pbs: L[PyBlock], q: Opt[Query]) -> L[PyBlock]:
        """Make dependency graph among PyBlocks and determine execution order"""
        from dbgen.utils.graphs import DiGraph, topsort_with_dict

        G = DiGraph()
        d = {pb.hash: pb for pb in pbs}
        G.add_nodes_from(d.keys())
        for pb in pbs:
            for arg_ind, a in enumerate(pb.args):
                if isinstance(a, Arg) and (not q or a.key != q.hash):
                    if a.key not in d:
                        raise KeyError(
                            f"Argument {arg_ind} of {pb.func.name} refers to an object with a hash key {a.key} asking for name \"{getattr(a,'name','<No Name>')}\" that does not exist in the namespace."
                            "Did you make sure to include all PyBlocks in the func kwarg of Generator()?"
                        )
                    assert a.key in d, pb.func.name
                    G.add_edge(a.key, pb.hash)
        return topsort_with_dict(G, d)

    def _get_all_saved_key_dict(self) -> D[str, S[str]]:
        saved_keys = {}  # type: D[str,S[str]]
        for act in self.loads:
            for hash_loc, name_set in self._get_saved_key_dict(act).items():
                saved_keys.update({hash_loc: {*name_set, *saved_keys.get(hash_loc, set())}})

        return saved_keys

    def _get_saved_key_dict(self, load: Load) -> D[str, S[str]]:
        saved_keys = {}  # type: D[str,S[str]]
        if load.pk:
            if isinstance(load.pk, Arg):
                hash_loc = load.pk.key
                arg_name = load.pk.name
                saved_keys.update({hash_loc: {arg_name, *saved_keys.get(hash_loc, set())}})

        for val in load.attrs.values():
            if isinstance(val, Arg):
                saved_keys.update({val.key: {val.name, *saved_keys.get(val.key, set())}})

        for fk_load in load.fks.values():
            for hash_loc, name_set in self._get_saved_key_dict(fk_load).items():
                saved_keys.update({hash_loc: {*name_set, *saved_keys.get(hash_loc, set())}})

        return saved_keys

    # ######################
    # # Airflow Operator Exports
    # # --------------------
    def operator(self, model_name: str, run: int, universe: D[str, Entity]) -> str:

        # Get the necessary template
        from dbgen.templates import jinja_env

        gen_template = jinja_env.get_template("generator.py.jinja")

        # Prepare the rendered arguments
        pbs = [("pb" + str(pb.hash).replace("-", "neg"), pb.hash, pb.make_src()) for pb in self.transforms]
        loaders = [loader.make_src() for loader in self.loads]
        objs = {oname: (o.id_str, repr(o.ids()), repr(o.id_fks())) for oname, o in universe.items()}
        consttransforms = [cf.src for cf in self._constargs()]

        # Set the template arguements
        template_kwargs = dict(
            name=self.name,
            pyblocks=pbs,
            genname=self.name,
            loads=loaders,
            objs=objs,
            query=self.query.showQ() if self.query else False,
            queryhash=self.query.hash if self.query else None,
            run=run,
            model_name=model_name,
            consttransforms=consttransforms,
        )

        return gen_template.render(**template_kwargs)
