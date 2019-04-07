 # External
from typing import (Set    as S,
                    List   as L,
                    Union  as U,
                    Tuple  as T)
from networkx            import DiGraph       # type: ignore
from networkx.algorithms import simple_cycles # type: ignore

# Internal
from dbgen.core.model.run_gen   import run_gen
from dbgen.core.model.run       import run, check_patheq,validate_name
from dbgen.core.model.metatable import make_meta

from dbgen.core.gen        import Gen
from dbgen.core.action     import Action
from dbgen.core.funclike   import PyBlock
from dbgen.core.schema     import Obj, Rel, RelTup, Path, PathEQ, Attr, View, RawView, QView, AttrTup
from dbgen.core.schemaclass import Schema
from dbgen.core.misc       import ConnectInfo as ConnI
from dbgen.utils.misc      import Base
from dbgen.utils.sql       import select_dict
from dbgen.utils.graphs    import topsort_with_dict
################################################################################
# Type Synonyms
Stuff = U[L[Obj],L[Rel],L[str],L[AttrTup],L[RelTup],L[Gen],L[View],L[PathEQ],L[T[str,Attr]],
          L[U[Obj,Rel,RelTup,AttrTup,str,Gen,PathEQ,View]]]
##########################################################################################
class Model(Schema):
    '''
    Just a named container for objects, relations, and generators

    Also, path equivalencies: which can be checked ad hoc
    '''
    def __init__(self,
                 name : str,
                 objs : L[Obj]    = None,
                 rels : L[Rel]    = None,
                 gens : L[Gen]    = None,
                 views: L[View]   = None,
                 pes  : L[PathEQ] = None,
                ) -> None:

        self.name = name
        self.objs = {o.name : o for o in (objs or [])}
        self.gens = {g.name : g for g in (gens or [])}
        self.views = {v.name : v for v in (views or [])}

        self._fks = DiGraph()
        self._fks.add_edges_from(self.objs) # nodes are object NAMES

        self.pe   = set(pes or []) # path equivalencies

        for rel in rels or []:
            self._add_relation(rel)

    def __str__(self) -> str:
        p = '%d objs' % len(self.objs)
        n = self._fks.number_of_edges()
        r = '%d rels' % n if n else ''
        m = '%d gens' % len(self.gens) if self.gens else ''
        e = '%d PathEQs'%len(self.pe) if self.pe else ''

        things = ', '.join(filter(None,[p,r,m,e]))
        return 'Model<%s,%s>'%(self.name,things)

    ######################
    # Externally defined #
    ######################
    run              = run
    _validate_name   = validate_name
    _run_gen         = run_gen
    _make_metatables = make_meta
    _check_patheq    = check_patheq

    ##################
    # Public methods #
    ##################
    def test_funcs(self) -> None:
        '''Run all PyBlock tests'''
        for g in self.gens.values():
            for f in g.funcs:
                f.test()

    def check_paths(self, db : ConnI) -> None:
        '''Use ASSERT statements to verify one's path equalities are upheld'''
        for pe in self.pe:
            self._check_patheq(pe, db)

    def get(self, objname : str) -> Obj:
        '''Get an object by name'''
        return self[objname]

    def rename(self, x : U[Obj,Rel,RelTup,AttrTup,str,Gen], name : str) -> None:
        '''Rename an Objects / Relations / Generators / Attr '''
        if isinstance(x,(Obj,str)):
            if isinstance(x,str):
                o = self[x]
            else:
                o = x
            self._rename_object(o,name)
        elif isinstance(x,(Rel,RelTup)):
            r = x if isinstance(x,Rel) else self.get_rel(x)
            self._rename_relation(r,name)
        elif isinstance(x,Gen):     self._rename_gen(x,name)
        elif isinstance(x,AttrTup): self._rename_attr(x,name)
        else:
            raise TypeError('A %s (%s) was passed to rename' % (type(x),x))

    def add(self, stuff : Stuff) -> None:
        '''Add a list containing Objects / Relations / Generators / PathEQs '''
        for x in stuff:
            if isinstance(x,(Obj,str)):
                if isinstance(x,str):
                    o = self[x]
                else:
                    o = x
                self._add_object(o)
            elif isinstance(x,(Rel,RelTup)):
                r = x if isinstance(x,Rel) else self.get_rel(x)
                self._add_relation(r)
            elif isinstance(x,Gen):    self._add_gen(x)
            elif isinstance(x,View):   self._add_view(x)
            elif isinstance(x,PathEQ): self._add_patheq(x)
            elif isinstance(x,tuple) and isinstance(x[1],Attr):
                assert isinstance(x[0],str)
                self._add_attr(x[0],x[1])
            else:
                raise TypeError('A %s (%s) was passed to add' % (type(x),x))

    def remove(self, stuff : Stuff ) -> None:
        '''Remove items given a list of Objects / Relations / Gens / PathEQs'''
        for x in stuff:
            if isinstance(x,(Obj,str)):
                if isinstance(x,str):
                    o = self[x]
                else:
                    o = x
                self._del_object(o)
            elif isinstance(x,(Rel,RelTup)):
                r = x if isinstance(x,Rel) else self.get_rel(x)
                self._del_relation(r)
            elif isinstance(x,Gen):       self._del_gen(x)
            elif isinstance(x,View):      self._del_view(x)
            elif isinstance(x,AttrTup):  self._del_attr(x)
            elif isinstance(x,PathEQ):    self._del_patheq(x)
            else:
                raise TypeError('A %s (%s) was passed to remove' % (type(x),x))

    # def randGenObj(self, o : Obj, n : int) -> T[PyBlock,L[Action]]:
    #     '''PyBlock to generate a random instance of an object from data'''
    #     raise NotImplementedError
        # aa = o.initattr() # get initial attributes L[Attr]
        # outputs = [[a.dtype.rand() for _ in range(n)] for a in aa]
        # data =  tuple(outputs) if len(outputs)>1 else outputs[0] # type: ignore
        # fname = 'randgen_' + o.name
        # src = 'def %s():\n\treturn '%fname+str(data)
        # pth = join(environ['DBGEN_TEMP'],fname+'.py')
        # with open(pth,'w') as f: f.write(src)
        # fun = Func.path_to_func(pth)
        # actions = [] # type: L[Action]
        # # IMPORTANT TO NOT DELETE THE PATH SO THAT GETSOURCE WORKS
        # return PyBlock(fun, outnames = [a.name for a in aa]),actions

    ###################
    # Private Methods #
    ###################

    @classmethod
    def _build_new(cls, name : str) -> "Model":
        '''Create a new model (used to generate meta.db)'''
        return cls(name)

    ##################################
    ### Adding/removing/renaming in model ###
    ##################################
    def _rename_gen(self, g : Gen, n : str) -> None:
        assert g == self.gens[g.name]
        new = g.copy()
        g.name = n
        del self.gens[g.name]
        self.gens[n] = g

    def _rename_attr(self, a : AttrTup, n : str) -> None:
        '''Replace object with one with a renamed attribute'''
        self.objs[a.obj] = self.objs[a.obj].rename_attr(a.name,n)
        # Make changes in generators?
        # Make changes in PathEQs?

    def _rename_object(self, o : Obj, n : str) -> None:
        '''Probably buggy'''
        assert o in self.objs.values(), 'Cannot delete %s: not found in model'%o
        oc = o.copy()
        oc.name = n
        del self.objs[o.name]
        self.objs[n] = oc

        for genname,g in self.gens.items():
            self.gens[genname] = g.rename_object(o,n)

        for fk in self._obj_all_fks(o):
            self._del_relation(fk)
            if fk.o1 == o.name: fk.o1 = n
            if fk.o2 == o.name: fk.o2 = n
            self._add_relation(fk)

    def _rename_relation(self, r : Rel, n: str) -> None:
        raise NotImplementedError


    def _del_gen(self, g : Gen) -> None:
        '''Delete a generator'''
        del self.gens[g.name]

    def _del_view(self, v : View) -> None:
        '''Delete a view'''
        del self.views[v.name]
        # need to delete generators/PathEQs

    def _del_attr(self, a : AttrTup) -> None:
        '''Delete an attribute: Need to also remove all Generators that mention it?'''
        self[a.obj].del_attrs([a.name])

    def _del_relation(self, r : Rel) -> None:
        ''' Remove from internal FK graph. Need to also remove all Generators that mention it? '''
        self._fks[r.o1][r.o2]['fks'].remove(r)
        if not self._fks[r.o1][r.o2]['fks']:
            self._fks.remove_edge(r.o1,r.o2)

        # Remove any path equivalencies that use this relation
        remove = set()
        for pe in self.pe:
            for p in pe:
                if r.tup() in p.rels:
                    remove.add(pe)
        self.pe -= remove

    def _del_object(self, o : Obj) -> None:
        '''Need to also remove all Generators that mention it?'''
        del self.objs[o.name]
        # Remove relations that mention object
        self._fks.remove_node(o.name)

        # Remove generators that mention object
        delgen = []
        for gn,g in self.gens.items():
            gobjs = g.dep().tabs_needed | g.dep().tabs_yielded
            if o.name in gobjs:
                delgen.append(gn)
        for gn in delgen:
            del self.gens[gn]

        # Remove path equivalencies that mention object
        remove = set()
        for pe in self.pe:
            for p in pe:
                pobjs = set([getattr(p.attr,'obj')] + [r.obj for r in p.rels])
                if o.name in pobjs:
                    remove.add(pe)

        self.pe -= remove

    def _del_patheq(self, peq : PathEQ) -> None:
        self.pe.remove(peq)

    def _add_gen(self, g : Gen) -> None:
        '''Add to model'''
        # Validate
        #--------
        if g.name in self.gens:
            raise ValueError('Cannot add %s, name already taken!' % g)
        # Add
        #----
        for a in g.actions:
            self._validate_action(a)

        self.gens[g.name] = g

    ###################
    ### Gen related ###
    ###################
    def _validate_action(self, a : Action) -> None:
        '''
        It is assumed that an Action provides all identifying data for an
        object, so that either its PK can be selected OR we can insert a new
        row...however this depends on the global model state (identifying
        relations can be added) so a model-level validation is necessary
        The action __init__ already verifies all ID attributes are present
        '''
        # Check all identifying relationships are covered
        if not a.pk: # don't have a PK, so need identifying info
            for fk in self._obj_fks(a.obj):
                if fk.id:
                    err = '%s missing identifying relation %s'
                    assert fk.name in a.fks, err%(a, fk)

        # Recursively validate sub-actions
        for act in a.fks.values():
            self._validate_action(act)

    def ordered_gens(self) -> L[Gen]:
        ''' Determine execution order of generators '''

        # Check for cycles:
        sc = list(simple_cycles(self._gen_graph()))

        if sc:
            small = min(sc, key = len)
            print('\nGenerator cycle found! Smallest cycle:')
            for gname in small:
                g = self.gens[gname]
                print('\n################\n',g.name)
                for k,v in vars(g.dep()).items():
                    print(k,v)
            import pdb;pdb.set_trace(); assert False

        # Use topological sort to order
        return list(topsort_with_dict(self._gen_graph(),self.gens))

    def _gen_graph(self) -> DiGraph:
        ''' Make a graph out of generator dependencies '''
        G = DiGraph()

        ddict = {a:g.dep() for a,g in self.gens.items()}
        G.add_nodes_from(list(self.gens.keys()))
        for a1 in self.gens.keys():
            d1 = ddict[a1]
            for a2 in self.gens.keys():
                d2 = ddict[a2]
                if a1!=a2 and d1.test(d2):
                    G.add_edge(a2, a1)
        return G

    def _todo(self) -> S[str]:
        ''' All attributes that do not yet have a generator populating them '''
        allattr = set()
        for o in self.objs.values():
            allattr.update([o.name+'.'+a for a in o.attrnames()])
        for r in self._rels():
            allattr.add(r.o1+'.'+r.name)
        alldone = set()
        for g in self.gens.values():
            alldone.update(g.dep().cols_yielded)
        return allattr - alldone
