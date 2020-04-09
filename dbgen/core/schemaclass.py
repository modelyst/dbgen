# External
from typing import Set as S, List as L, Dict as D, Union as U, Tuple as T, Callable as C
from copy import deepcopy
from networkx import DiGraph  # type: ignore
from tqdm import tqdm  # type: ignore
from hypothesis.strategies import (
    SearchStrategy,
    builds,
    lists,
    composite,  # type: ignore
    just,
    sampled_from,
)
from hypothesis import infer, assume  # type: ignore

# Internal
from dbgen.utils.misc import Base, nonempty, letters
from dbgen.core.schema import (
    Obj,
    Rel,
    RelTup,
    Path,
    PathEQ,
    Attr,
    View,
    RawView,
    QView,
    AttrTup,
)
from dbgen.core.misc import ConnectInfo as ConnI
from dbgen.utils.sql import sqlexecute, sqlselect

############################################################################


class Schema(Base):
    """Unnamed collection of Objects, Views, Relations, Path Equations"""

    def __init__(
        self, objlist: L[Obj] = None, viewlist: L[View] = None, pes: L[PathEQ] = None,
    ) -> None:

        self.objlist = objlist or []
        self.viewlist = viewlist or []

        self._fks = DiGraph()
        self._fks.add_nodes_from(self.objlist)  # nodes are object NAMES

        self.pes = set(pes or [])  # path equivalencies

        for o in self.objlist:
            for rel in o.fks:
                self._add_relation(rel.to_rel(o.name))

        super().__init__()

    def __str__(self) -> str:
        return "Schema<%d objs, %d rels>" % (len(self.objlist), len(self._fks))

    def __getitem__(self, key: str) -> Obj:
        return self.objs[key.lower()]

    def __contains__(self, key: str) -> bool:
        return key.lower() in self.objs

    @property
    def objs(self) -> D[str, Obj]:
        return {o.name.lower(): o for o in self.objlist}

    @property
    def views(self) -> D[str, View]:
        return {o.name.lower(): o for o in self.viewlist}

    @classmethod
    @composite
    def strat(draw: C, cls: "Schema") -> SearchStrategy:
        MAX_OBJ = 2
        MAX_FK = 2
        objnames = draw(lists(letters, min_size=1, max_size=MAX_OBJ, unique=True))
        objlist = []  # type: L[Obj]
        for o in objnames:
            fktargets = draw(lists(sampled_from(objnames), min_size=1, max_size=MAX_FK))
            n = len(fktargets)
            fknames = draw(
                lists(letters, min_size=n, max_size=n, unique=True).filter(
                    lambda fkn: o not in fkn
                )
            )
            objlist.append(draw(Obj.strat(name=o, fks=list(zip(fknames, fktargets)))))
        return draw(builds(cls, objlist=just(objlist)))

    def make_schema(self, conn: ConnI, nuke: str = "", bar: bool = True) -> None:
        """Create empty schema."""
        if nuke.lower() in ["t", "true"]:
            safe_conn = deepcopy(conn)
            safe_cxn = safe_conn.connect()
            sqlexecute(safe_cxn, f'DROP SCHEMA IF EXISTS "{conn.schema}" CASCADE')
            sqlexecute(safe_cxn, f'CREATE SCHEMA "{conn.schema}"')
        cxn = conn.connect()
        tqargs = dict(leave=False, disable=not bar)
        # Add the functions
        self.add_functions(conn)

        for ta in tqdm(self.objs.values(), desc="adding tables", **tqargs):
            if not getattr(ta, "_is_view", False):
                for sql in ta.create():
                    sqlexecute(cxn, sql)
        for r in tqdm(self._rels(), desc="adding relations", **tqargs):
            sqlexecute(cxn, self._create_fk(r))

        for vn in tqdm(self.views, desc="adding views", **tqargs):
            q = 'CREATE OR REPLACE VIEW "{}" AS {};'.format(vn, self._show_view(vn))
            sqlexecute(cxn, q)

    def check_schema_exists(self, conn: ConnI) -> bool:
        cxn = conn.connect()
        q = f"SELECT table_name from information_schema.tables where \
        table_schema = '{conn.schema}'"
        tables_in_db = [x[0] for x in sqlselect(cxn, q)]
        return all([obj in tables_in_db for obj in self.objs])

    def add_functions(self, conn: ConnI) -> bool:
        statement = f"""
        CREATE OR REPLACE FUNCTION create_constraint_if_not_exists (t_name text, c_name text, constraint_sql text)
  RETURNS void
AS
$BODY$
  begin
    -- Look for our constraint
    if not exists (select constraint_name
                   from information_schema.table_constraints tc 
where
    tc.constraint_name = c_name
    and tc.table_name = t_name
    and tc.table_schema = '{conn.schema}') then
        execute 'ALTER TABLE ' || t_name || ' ADD CONSTRAINT ' || c_name || ' ' || constraint_sql;
    end if;
end;
$BODY$
LANGUAGE plpgsql VOLATILE;
        """
        cxn = conn.connect()
        sqlexecute(cxn, statement)
        return True

    ################
    # Adding stuff #
    ################

    def _add_attr(self, oname: str, a: Attr) -> None:
        """Add to model"""
        self[oname].attrs.append(a)

    def _add_object(self, o: Obj) -> None:
        """Add to model"""

        if o.name in self:  # Validate
            raise ValueError("Cannot add %s, name is already taken!" % o)
        else:
            self.objlist.append(o)  # Add
            self._fks.add_node(o.name)

        for rel in o.fkdict.values():
            self._add_relation(rel)

    def _add_view(self, v: View) -> None:
        """Add to model"""
        # Validate
        # --------
        if v.name in self.views or v.name in self.objs:
            raise ValueError("Cannot add %s, name already taken!" % v)
        # Add
        # ----
        self.viewlist.append(v)

    def _add_relation(self, r: Rel) -> None:
        """Add to model"""
        # Validate
        # ---------
        assert isinstance(r, Rel)
        err = "Cannot add %s: %s not found in model %s"
        assert r.o1 in self, err % (r, r.o1, self.objlist)
        assert r.o2 in self, err % (r, r.o2, self.objlist)

        notfound = "Cannot add %s, %s already has a %s with that name "

        for _, _, data in self._fks.edges(r.o1, data=True):
            if r.name in [fk.name for fk in data["fks"]]:
                raise ValueError(notfound % (r, r.o1, "relation"))
            if r.name in self[r.o1].attrnames():
                raise ValueError(notfound % (r, r.o1, "attribute"))

        # Add
        # ----
        self.add_fk(self._fks, r)

    def _add_patheq(self, peq: PathEQ) -> None:
        """Add to model"""
        # Validate
        # ---------
        rand = next(iter(peq))
        start = rand.start()
        end = rand._path_end(self)
        for p in peq:
            self._validate_path(p)
            assert p.start() == start
            assert p._path_end(self) == end

        self.pes.add(peq)

    @staticmethod
    def add_fk(G: DiGraph, r: Rel, forward: bool = True) -> None:
        """ Modify a graph (used by both add_relation and info_graph) """
        a, b = (r.o1, r.o2)
        if not forward:
            a, b = b, a
        if G.has_edge(a, b):
            G[a][b]["fks"].append(r)
            G[a][b]["fks"] = sorted(G[a][b]["fks"], key=lambda x: repr(x))
        else:
            G.add_edge(a, b, fks=list([r]))

    ###########
    # Objects #
    ###########
    def add_cols(self, obj: Obj) -> L[str]:
        attr_stmts = []
        for c in obj.attrs:
            obj_name = obj.name
            col_name, col_desc = c.create_col(obj.name)
            stmt = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s" % (obj.name, col_name)
            attr_stmts.append(stmt)
            attr_stmts.append(col_desc)
        rel_stmts = [self._create_fk(rel) for rel in obj.fkdict.values()]
        return attr_stmts + rel_stmts

    #########
    # Exprs #
    #########
    def all_attr(self) -> S[AttrTup]:
        out = set()
        for o in self.objs.values():
            for a in o.attrnames():
                out.add(o[a])
        return out

    ################
    # FK Graph #
    ################
    def _create_fk(self, fk: Rel) -> str:
        """create SQL FK statement"""
        args = [fk.o1, fk.name, fk.o2, self[fk.o2].id_str]
        stmt = 'ALTER TABLE "{0}" ADD COLUMN IF NOT EXISTS "{1}" BIGINT;'
        stmt += 'ALTER TABLE "{0}" DROP CONSTRAINT IF EXISTS fk__{0}__{1}__{2}__{3};'
        stmt += 'ALTER TABLE "{0}" ADD CONSTRAINT fk__{0}__{1}__{2}__{3}\
        FOREIGN KEY ("{1}") REFERENCES "{2}"("{3}");'
        stmt += 'CREATE INDEX IF NOT EXISTS "{0}__{1}__fkey" ON "{0}"("{1}");'
        return stmt.format(*args)

    def _rels(self) -> S[Rel]:
        """ ALL relations between any objects, in some order """
        fks = set()
        for _, _, d in self._fks.edges(data=True):
            for fk in d["fks"]:
                fks.add(fk)
        return fks

    def _obj_all_fks(self, o: Obj) -> S[Rel]:
        """ Relations that start OR end on a given object """
        inward = set.union(
            *[d["fks"] for _, _, d in self._fks.in_edges(o.name, data=True)]
        )
        return set(o.fkdict.values()) | inward

    def get_rel(self, r: U[Rel, RelTup]) -> Rel:
        """
        Upgrade a Relation representation that only has limited
        (but identifying) info
        """
        if isinstance(r, Rel):
            return r
        else:
            return self[r.obj].fkdict[r.rel]

    def _info_graph(self, links: L[U[Rel, RelTup]]) -> DiGraph:
        """Natural paths of information propagation, which includes the normal
            Rel relationships but also taking into account a
            user-specified list of relationships that are allowed to propagate
            information in the 'reverse' direction

            Furthermore, 1-1 relationships are identified and information is
            allowed to propagate in opposite direction there, too.
        """
        G = self._fks.copy()

        for name, o in self.objs.items():
            pars = [fk for fk in o.fkdict.values() if fk.identifying]
            # only if it's a 1-1 table
            if len(pars) == 1 and len(o.ids()) == 0:
                p = pars[0]  # identifying foreign key
                self.add_fk(G, p, forward=False)

        for fk in links:
            if isinstance(fk, RelTup):
                fk = self.get_rel(fk)
            self.add_fk(G, fk, forward=False)

        return G

    #########
    # Paths #
    #########
    def _validate_path(self, p: Path) -> None:
        """Throw error if invalid path is passed"""
        curr = self[p.start()]
        for r in p.rels:
            rel = self.get_rel(r)
            assert rel.o1 == curr.name
            curr = self[rel.o2]
        if hasattr(p, "attr"):
            assert getattr(p, "attr").obj == curr.name

    #########
    # Views #
    #########
    def _show_view(self, vname: str) -> str:
        v = self.views[vname]
        if isinstance(v, RawView):
            return v.raw
        elif isinstance(v, QView):
            return v.q.showQ()
        else:
            raise TypeError
