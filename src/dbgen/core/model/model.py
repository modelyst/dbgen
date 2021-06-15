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

"""Module for the DBgen Model object"""
from functools import reduce

# External
from typing import Any
from typing import Dict as D
from typing import List as L
from typing import Set as S
from typing import Tuple as T
from typing import Union as U

from hypothesis.strategies import SearchStrategy, just
from networkx import DiGraph

from dbgen.core.expr.sqltypes import SQLType
from dbgen.core.fromclause import Path as JPath
from dbgen.core.func import Env
from dbgen.core.gen import Generator
from dbgen.core.load import Load
from dbgen.core.misc import ConnectInfo as ConnI
from dbgen.core.model.metatable import make_meta
from dbgen.core.model.run import check_patheq, run, validate_name

# Internal
from dbgen.core.model.run_gen import run_gen
from dbgen.core.schema import Attr, AttrTup, Entity, Partition, PathEQ, Rel, RelTup, SuperRel, UserRel, View
from dbgen.core.schemaclass import Schema
from dbgen.utils.exceptions import DBgenInternalError

################################################################################
# Type Synonyms
Stuff = U[
    L[Entity],
    L[Rel],
    L[str],
    L[AttrTup],
    L[RelTup],
    L[Generator],
    L[View],
    L[PathEQ],
    L[T[str, Attr]],
    L[U[Entity, Rel, RelTup, AttrTup, str, Generator, PathEQ, View]],
]
UNIVERSE_TYPE = D[str, T[str, S[str], S[str], D[str, SQLType]]]
##########################################################################################


class Model(Schema):
    """
    Just a named container for objects, relations, and generators

    Also, path equivalencies: which can be checked ad hoc
    """

    def __init__(
        self,
        name: str,
        objlist: L[Entity] = None,
        genlist: L[Generator] = None,
        viewlist: L[View] = None,
        pes: L[PathEQ] = None,
    ) -> None:
        """
        Initialize model with list of objects, generators, views, and path
        equivalency. These lists can be appended to after initizialization
        through the model.add method

        Args:
            name (str): Name of the model used to uniquely identify the model
            objlist (L[Entity], optional): List of Entity objects. Defaults to [].
            genlist (L[Gen], optional): List of Generator objects. Defaults to [].
            viewlist (L[View], optional): List of View objects. Defaults to [].
            pes (L[PathEQ], optional): List of path equivalency objects. Defaults to [].
        """
        self.name = name
        self.objlist = objlist or []
        self.genlist = genlist or []
        self.viewlist = viewlist or []

        self._fks = DiGraph()
        self._fks.add_nodes_from(self.objs)  # nodes are object NAMES

        self.pes = set(pes or [])  # path equivalencies

        for o in self.objs.values():
            for rel in o.fks:
                self._add_relation(rel.to_rel(o.name))
        super(Schema, self).__init__()

    @property
    def gens(self) -> D[str, Generator]:
        return {g.name: g for g in self.genlist}

    @property
    def env(self) -> Env:
        return reduce(lambda first, second: first + second.env, self.genlist, Env())

    def __str__(self) -> str:
        p = "%d objs" % len(self.objs)
        n = self._fks.number_of_edges()
        r = "%d rels" % n if n else ""
        m = "%d gens" % len(self.gens) if self.gens else ""
        e = "%d PathEQs" % len(self.pes) if self.pes else ""

        things = ", ".join(filter(None, [p, r, m, e]))
        return f"Model<{self.name},{things}>"

    @classmethod
    def _strat(cls) -> SearchStrategy:
        """A hypothesis strategy for generating random examples."""
        objs = [
            Entity("a", attrs=[Attr("aa")], fks=[UserRel("ab", "b")]),
            Entity("b", attrs=[Attr("bb")]),
        ]
        gens = [Generator(name="pop_a", transforms=[], loads=[], tags=["io"])]
        return just(cls(name="model", objlist=objs, genlist=gens))

    ######################
    # Externally defined #
    ######################
    run = run

    _validate_name = validate_name
    _run_gen = run_gen
    _make_metatables = make_meta
    _check_patheq = check_patheq

    def run_airflow(self, *args, **kwargs):
        try:
            from dbgen.core.model.run_airflow import run_airflow
        except ImportError as exc:
            print(
                "Import error on model.run_airflow call, apache-airflow is required"
                "for running a model using airflow (This is highly experimental "
                "feature right now)"
            )
            raise exc
        run_airflow(self, *args, **kwargs)

    ##################
    # Public methods #
    ##################
    def test_gen(
        self,
        gen_name: str,
        db: ConnI,
        interact: bool = True,
        limit: int = 5,
    ) -> T[L[D[str, dict]], L[D[str, L[dict]]]]:
        assert gen_name in self.gens, f"Generator {gen_name} not in model:\n{self.gens.keys()}"

        return self.gens[gen_name].test_with_db(
            universe=self._get_universe(), db=db, interact=interact, limit=limit
        )

    def test_transforms(self) -> None:
        """Run all PyBlock tests"""
        for g in self.gens.values():
            for f in g.transforms:
                f.test()

    def check_paths(self, db: ConnI) -> None:
        """Use ASSERT statements to verify one's path equalities are upheld"""
        for pe in self.pes:
            self._check_patheq(pe, db)

    def make_path(self, end: U[str, "Entity"], rels: list = None, name: str = None) -> JPath:
        # Upgrade End
        # Change end into object if it is a string
        if isinstance(end, str):
            upgraded_end = self[end]
        elif isinstance(end, Partition):
            raise NotImplementedError()
        else:
            assert isinstance(end, Entity)
            upgraded_end = end

        # UPGRADE FKS
        def upgrade_rels(fs: list):
            res = []
            for rel_or_list in fs:
                if isinstance(rel_or_list, RelTup):
                    rel_or_list = self.get_rel(rel_or_list)
                if isinstance(rel_or_list, Rel):
                    res.append(
                        SuperRel(
                            rel_or_list.name,
                            rel_or_list.o1,
                            rel_or_list.o2,
                            self[rel_or_list.o2].id_str,
                        )
                    )
                else:
                    res.append(upgrade_rels(rel_or_list))
            return res

        upgraded_fks = upgrade_rels(rels) if rels else []

        return JPath(upgraded_end, upgraded_fks, name=name)

    def get(self, objname: str, partition_val: Any = None) -> Entity:
        """Get an object by name"""
        return self.get_entity(objname, partition_val)

    def rename(self, x: U[Entity, Rel, RelTup, AttrTup, str, Generator], name: str) -> None:
        """Rename an Objects / Relations / Generators / Attr """
        if isinstance(x, (Entity, str)):
            if isinstance(x, str):
                o = self[x]
            else:
                o = x
            self._rename_object(o, name)
        elif isinstance(x, (Rel, RelTup)):
            r = x if isinstance(x, Rel) else self.get_rel(x)
            self._rename_relation(r, name)
        elif isinstance(x, Generator):
            self._rename_gen(x, name)
        elif isinstance(x, AttrTup):
            self._rename_attr(x, name)
        else:
            raise TypeError(f"A {type(x)} ({x}) was passed to rename")

    def add(self, stuff: Stuff) -> None:
        """Add a list containing Objects / Relations / Generators / PathEQs """
        for x in stuff:
            if isinstance(x, (Entity, str)):
                if isinstance(x, str):
                    o = self[x]
                else:
                    o = x
                self._add_object(o)
            elif isinstance(x, (Rel, RelTup)):
                r = x if isinstance(x, Rel) else self.get_rel(x)
                self._add_relation(r)
            elif isinstance(x, Generator):
                self._add_gen(x)
            elif isinstance(x, View):
                self._add_view(x)
            elif isinstance(x, PathEQ):
                self._add_patheq(x)
            elif isinstance(x, tuple) and isinstance(x[1], Attr):
                assert isinstance(x[0], str)
                self._add_attr(x[0], x[1])
            else:
                raise TypeError(f"A {type(x)} ({x}) was passed to add")

    def remove(self, stuff: Stuff) -> None:
        """Remove items given a list of Objects / Relations / Gens / PathEQs"""
        for x in stuff:
            if isinstance(x, (Entity, str)):
                if isinstance(x, str):
                    o = self[x]
                else:
                    o = x
                self._del_object(o)
            elif isinstance(x, (Rel, RelTup)):
                r = x if isinstance(x, Rel) else self.get_rel(x)
                self._del_relation(r)
            elif isinstance(x, Generator):
                self._del_gen(x)
            elif isinstance(x, View):
                self._del_view(x)
            elif isinstance(x, PathEQ):
                self._del_patheq(x)
            else:
                raise TypeError(f"A {type(x)} ({x}) was passed to remove")

    ###################
    # Private Methods #
    ###################

    @classmethod
    def _build_new(cls, name: str) -> "Model":
        """Create a new model (used to generate meta.db)"""
        return cls(name)

    ##################################
    # Adding/removing/renaming in model
    ##################################
    def _rename_gen(self, g: Generator, n: str) -> None:
        assert g == self.gens[g.name]
        g.name = n
        del self.gens[g.name]
        self.gens[n] = g

    def _rename_attr(self, a: AttrTup, n: str) -> None:
        """Replace object with one with a renamed attribute"""
        self.objs[a.obj] = self.objs[a.obj].rename_attr(a.name, n)
        # Make changes in generators?
        # Make changes in PathEQs?

    def _rename_object(self, o: Entity, n: str) -> None:
        """Probably buggy"""
        assert o in self.objs.values(), f"Cannot delete {o}: not found in model"
        oc = o.copy()
        oc.name = n
        del self.objs[o.name]
        self.objs[n] = oc

        for genname, g in self.gens.items():
            self.gens[genname] = g.rename_object(o, n)

        for fk in self._obj_all_fks(o):
            self._del_relation(fk)
            if fk.o1 == o.name:
                fk.o1 = n
            if fk.o2 == o.name:
                fk.o2 = n
            self._add_relation(fk)

    def _rename_relation(self, r: Rel, n: str) -> None:
        raise NotImplementedError

    def _del_gen(self, g: Generator) -> None:
        """Delete a generator"""
        del self.gens[g.name]

    def _del_view(self, v: View) -> None:
        """Delete a view"""
        del self.views[v.name]
        # need to delete generators/PathEQs

    def _del_relation(self, r: Rel) -> None:
        """ Remove from internal FK graph. Need to also remove all Generators that mention it? """
        self._fks[r.o1][r.o2]["fks"].remove(r)
        if not self._fks[r.o1][r.o2]["fks"]:
            self._fks.remove_edge(r.o1, r.o2)

        # Remove any path equivalencies that use this relation
        remove = set()
        for pe in self.pes:
            for p in pe:
                if r.tup() in p.rels:
                    remove.add(pe)
        self.pes -= remove

    def _del_object(self, o: Entity) -> None:
        """Need to also remove all Generators that mention it?"""
        del self.objs[o.name]
        # Remove relations that mention object
        self._fks.remove_node(o.name)

        # Remove generators that mention object
        delgen = []
        for gn, g in self.gens.items():
            gobjs = g.dep(self.objs).tabs_needed | g.dep(self.objs).tabs_yielded
            if o.name in gobjs:
                delgen.append(gn)
        for gn in delgen:
            del self.gens[gn]

        # Remove path equivalencies that mention object
        remove = set()
        for pe in self.pes:
            for p in pe:
                pobjs = set([getattr(p.attr, "obj")] + [r.obj for r in p.rels])
                if o.name in pobjs:
                    remove.add(pe)

        self.pes -= remove

    def _del_patheq(self, peq: PathEQ) -> None:
        self.pes.remove(peq)

    def _add_gen(self, g: Generator) -> None:
        """Add to model"""
        # Validate
        # --------
        if g.name in self.gens:
            raise ValueError(f"Cannot add {g}, name already taken!")
        # Add
        # ----
        for a in g.loads:
            self._validate_load(a)

        self.genlist.append(g)

    ###################
    # Generator related ###
    ###################
    def _validate_load(self, a: Load) -> None:
        """
        It is assumed that an Load provides all identifying data for an
        object, so that either its PK can be selected OR we can insert a new
        row...however this depends on the global model state (identifying
        relations can be added) so a model-level validation is necessary
        The load __init__ already verifies all ID attributes are present
        """
        # Check all identifying relationships are covered
        if not a.pk:  # don't have a PK, so need identifying info
            for fk in self[a.obj].id_fks():
                err = "%s missing identifying relation %s"
                assert fk in a.fks, err % (a, fk)

        # Recursively validate sub-loads
        for act in a.fks.values():
            self._validate_load(act)

    def ordered_gens(self) -> L[Generator]:
        """ Determine execution order of generators """

        # Check for cycles:
        from networkx.algorithms import simple_cycles

        from dbgen.utils.graphs import topsort_with_dict

        sc = list(simple_cycles(self._gen_graph()))

        if sc:
            small = min(sc, key=len)
            print("\nGenerator cycle found! Smallest cycle:")
            for gname in small:
                g = self.gens[gname]
                print("\n################\n", g.name)
                for k, v in vars(g.dep(self.objs)).items():
                    print(k, v)
            raise DBgenInternalError("Found Cycle")

        # Use topological sort to order
        return list(topsort_with_dict(self._gen_graph(), self.gens))

    def _gen_graph(self) -> "DiGraph":
        """ Make a graph out of generator dependencies."""
        from networkx import DiGraph

        G = DiGraph()

        ddict = {a: g.dep(self.objs) for a, g in self.gens.items()}
        G.add_nodes_from(list(self.gens.keys()))
        for a1 in self.gens.keys():
            d1 = ddict[a1]
            for a2 in self.gens.keys():
                d2 = ddict[a2]
                if a1 != a2 and d1.test(d2):
                    G.add_edge(a2, a1)
        return G

    def _todo(self) -> S[str]:
        """ All attributes that do not yet have a generator populating them """
        allattr = set()
        for o in self.objs.values():
            allattr.update([o.name + "." + a for a in o.attrnames()])
        for r in self._rels():
            allattr.add(r.o1 + "." + r.name)
        alldone = set()
        for g in self.gens.values():
            alldone.update(g.dep(self.objs).cols_yielded)
        return allattr - alldone

    def _get_universe(self) -> UNIVERSE_TYPE:
        return {
            oname: (
                o.id_str,
                set(o.ids()),
                set(o.id_fks()),
                {key: val.dtype for key, val in o.attrdict.items()},
            )
            for oname, o in self.objs.items()
        }
