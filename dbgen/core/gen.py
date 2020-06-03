from typing import (
    Any,
    TYPE_CHECKING,
    List as L,
    Dict as D,
    Optional as Opt,
    Set as S,
    Tuple as T,
)

from networkx import DiGraph  # type: ignore
from hypothesis.strategies import SearchStrategy, builds, lists  # type: ignore


from dbgen.core.func import Env, defaultEnv, Func
from dbgen.core.funclike import PyBlock, Arg
from dbgen.core.action import Action
from dbgen.core.query import Query
from dbgen.core.misc import Dep
from dbgen.core.schema import Obj

from dbgen.utils.graphs import topsort_with_dict
from dbgen.utils.misc import Base, nonempty
from dbgen.utils.lists import concat_map
from dbgen.utils.str_utils import hash_
from dbgen.utils.sql import (
    Connection as Conn,
    sqlexecute,
    mkSelectCmd,
    mkUpdateCmd,
    sqlselect,
    mkInsCmd,
)

from dbgen.templates import jinja_env

# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model

    Model
"""
Defines a Generator, as well as a Model method that is directly related
"""
################################################################################


class Gen(Base):
    """Generator: populates database with data"""

    def __init__(
        self,
        name: str,
        desc: str = None,
        query: Query = None,
        funcs: L[PyBlock] = None,
        actions: L[Action] = None,
        tags: L[str] = None,
        env: Env = None,
        batch_size: int = None,
    ) -> None:

        # assert actions, 'Cannot have generator which does nothing'
        assert name
        self.name = name.lower()
        self.desc = desc or "<no description>"
        self.query = query
        self.funcs = self._order_funcs(funcs or [], query)
        self.actions = actions or []
        self.tags = [t.lower() for t in tags or []]
        self.env = env or defaultEnv
        self.batch_size = batch_size
        for func in self.funcs:
            self.env += func.func.env
        super().__init__()

    def __str__(self) -> str:
        return "Gen<%s>" % self.name

    @classmethod
    def _strat(cls) -> SearchStrategy:
        """A hypothesis strategy for generating random examples."""
        return builds(
            cls,
            name=nonempty,
            desc=nonempty,
            query=Query._strat(),
            funcs=lists(PyBlock._strat(), max_size=2),
            actions=lists(Action._strat(), min_size=1, max_size=2),
            tags=lists(nonempty, max_size=3),
        )

    ##################
    # Public Methods #
    ##################

    def update_status(
        self, conn: Conn, run_id: int, status: str, err: str = ""
    ) -> None:
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

    def dep(self, universe: D[str, Obj]) -> Dep:
        """
        Determine the tabs/cols that are both inputs and outputs to the Gen 

        Args:
            universe (D[str, Obj])

        Returns:
            Dep
        """
        # Analyze allattr and allobj to get query dependencies
        if self.query:
            tabdeps = self.query.allobj()
            coldeps = ["%s.%s" % (a.obj, a.name) for a in self.query.allattr()]
            for r in self.query.allrels():
                coldeps.append(r.obj + "." + r.rel)
        else:
            tabdeps, coldeps = [], []

        # Analyze actions to see what new cols and tabs are yielded
        newtabs, newcols = [], []  # type: T[L[str],L[str]]

        for a in self.actions:
            tabdeps.extend(a.tabdeps())
            newtabs.extend(a.newtabs())
            newcols.extend(a.newcols(universe))

        # Allow for unethical hacks
        for t in self.tags:
            if t[:4] == "dep ":
                coldeps.append(t[4:])

        return Dep(tabdeps, coldeps, newtabs, newcols)

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

    def rename_object(self, o: Obj, n: str) -> "Gen":
        """Change all references to an object to account for name change"""
        g = self.copy()
        if g.query:
            g.query.basis = [n if b == o.name else b for b in g.query.basis]
        for i, a in enumerate(g.actions):
            g.actions[i] = a.rename_object(o, n)
        return g

    def purge(self, conn: Conn, mconn: Conn, universe: D[str, Obj]) -> None:
        """
        If a generator is purged, then any
        tables it populates will be truncated. Any columns it populates will be set all
        to NULL
        """
        d = self.dep(universe)
        tabs, cols = d.tabs_yielded, d.cols_yielded
        for t in tabs:
            sqlexecute(conn, "TRUNCATE {} CASCADE".format(t))

        for t, c in map(lambda x: x.split("."), cols):
            sqlexecute(mconn, "UPDATE {} SET {} = NULL".format(t, c))

        gids = sqlselect(mconn, "SELECT gen_id FROM gen WHERE name = %s", [self.name])
        for gid in gids:
            sqlexecute(mconn, "DELETE FROM repeats WHERE gen_id = %s", [gid])

    def test(self, input_rows: L[D[str, Any]], rename_dict: bool = True):
        # Apply the
        output_dicts = []
        for row in input_rows:
            result_dict = {self.query.hash: row} if self.query else {}
            for pb in self.funcs:
                result_dict[pb.hash] = pb(result_dict)

            # Replace pyblock hashes with function names if flag is True
            func_name_dict = {pb.hash: pb.func.name for pb in self.funcs}
            if rename_dict:
                result_dict = {
                    func_name_dict.get(key, "query"): val
                    for key, val in result_dict.items()
                }
            output_dicts.append(result_dict)

        return output_dicts

    ##################
    # Private Methods #
    ##################

    def _constargs(self) -> L[Func]:
        return concat_map(lambda y: y._constargs(), self.funcs)

    @staticmethod
    def _order_funcs(pbs: L[PyBlock], q: Opt[Query]) -> L[PyBlock]:
        """Make dependency graph among PyBlocks and determine execution order"""
        G = DiGraph()
        d = {pb.hash: pb for pb in pbs}
        G.add_nodes_from(d.keys())
        for pb in pbs:
            for arg_ind, a in enumerate(pb.args):
                if isinstance(a, Arg) and (not q or a.key != q.hash):
                    if a.key not in d:
                        raise KeyError(
                            f"Argument {arg_ind} of {pb.func.name} refers to an object with a hash key {a.key} asking for name \"{getattr(a,'name','<No Name>')}\" that does not exist in the namespace."
                            "Did you make sure to include all PyBlocks in the func kwarg of Gen()?"
                        )
                    assert a.key in d, pb.func.name
                    G.add_edge(a.key, pb.hash)
        return topsort_with_dict(G, d)

    def _get_all_saved_key_dict(self) -> D[str, S[str]]:
        saved_keys = {}  # type: D[str,S[str]]
        for act in self.actions:
            for hash_loc, name_set in self._get_saved_key_dict(act).items():
                saved_keys.update(
                    {hash_loc: set([*name_set, *saved_keys.get(hash_loc, set())])}
                )

        return saved_keys

    def _get_saved_key_dict(self, action: Action) -> D[str, S[str]]:
        saved_keys = {}  # type: D[str,S[str]]
        if action.pk:
            hash_loc = action.pk.key
            arg_name = action.pk.name
            saved_keys.update(
                {hash_loc: set([arg_name, *saved_keys.get(hash_loc, set())])}
            )

        for val in action.attrs.values():
            if isinstance(val, Arg):
                saved_keys.update(
                    {val.key: set([val.name, *saved_keys.get(val.key, set())])}
                )

        for fk_action in action.fks.values():
            for hash_loc, name_set in self._get_saved_key_dict(fk_action).items():
                saved_keys.update(
                    {hash_loc: set([*name_set, *saved_keys.get(hash_loc, set())])}
                )

        return saved_keys

    # ######################
    # # Airflow Operator Exports
    # # --------------------
    def operator(self, model_name: str, run: int, universe: D[str, Obj]) -> str:

        # Get the necessary template
        gen_template = jinja_env.get_template("generator.py.jinja")

        # Prepare the rendered arguments
        pbs = [
            ("pb" + str(pb.hash).replace("-", "neg"), pb.hash, pb.make_src())
            for pb in self.funcs
        ]
        loaders = [loader.make_src() for loader in self.actions]
        objs = {
            oname: (o.id_str, repr(o.ids()), repr(o.id_fks()))
            for oname, o in universe.items()
        }
        constfuncs = [cf.src for cf in self._constargs()]

        # Set the template arguements
        template_kwargs = dict(
            name=self.name,
            pyblocks=pbs,
            genname=self.name,
            env=str(self.env) if self.env else "",
            loads=loaders,
            objs=objs,
            query=self.query.showQ() if self.query else False,
            queryhash=self.query.hash if self.query else None,
            run=run,
            model_name=model_name,
            constfuncs=constfuncs,
        )

        return gen_template.render(**template_kwargs)
