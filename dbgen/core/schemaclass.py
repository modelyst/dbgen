 # External
from typing import (Set    as S,
                    List   as L,
                    Union  as U,
                    Tuple  as T)
from copy     import deepcopy
from networkx import DiGraph     # type: ignore
from tqdm     import tqdm        # type: ignore

# Internal
from dbgen.utils.misc  import Base
from dbgen.core.schema import Obj, Rel, RelTup, Path, PathEQ, Attr, View, RawView, QView,AttrTup
from dbgen.core.misc   import ConnectInfo as ConnI
from dbgen.core.pathconstraint import Path as JPath, Constraint
from dbgen.utils.sql   import sqlexecute, sqlselect, Error
############################################################################
class Schema(Base):
    '''Unnamed collection of Objects, Views, Relations, Path Equations'''
    def __init__(self,
                 objs : L[Obj]    = None,
                 rels : L[Rel]    = None,
                 views: L[View]   = None,
                 pes  : L[PathEQ] = None,
                ) -> None:

        self.objs = {o.name : o for o in (objs or [])}
        self.views = {v.name : v for v in (views or [])}

        self._fks = DiGraph()
        self._fks.add_edges_from(self.objs) # nodes are object NAMES

        self.pe   = set(pes or []) # path equivalencies

        for rel in rels or []:
            self._add_relation(rel)


    def __getitem__(self, key : str) -> Obj:
        return self.objs[key.lower()]

    def __contains__(self,key : str)->bool:
        return key.lower() in self.objs

    def make_schema(self,
                    conn : ConnI,
                    nuke : bool = True,
                    bar : bool = True
                    ) -> None:
        '''
        Create empty schema
        '''
        if nuke:
            safe_conn    = deepcopy(conn)
            safe_conn.db = 'postgres'
            safe_cxn = safe_conn.connect()
            sqlexecute(safe_cxn,'DROP DATABASE IF EXISTS '+conn.db)
            sqlexecute(safe_cxn,'CREATE DATABASE '+conn.db)
        cxn = conn.connect()
        tqargs = dict(leave=False,disable=not bar)
        for ta in tqdm(self.objs.values(),desc='adding tables',**tqargs):
            if not getattr(ta,'_is_view',False):
                for sql in ta.create(): sqlexecute(cxn,sql)
        for r in tqdm(self._rels(),desc='adding relations',**tqargs):
                sqlexecute(cxn,self._create_fk(r))

        for vn in tqdm(self.views,desc='adding views',**tqargs):
            q = 'CREATE OR REPLACE VIEW "{}" AS {};'.format(vn,self._show_view(vn))
            sqlexecute(cxn,q)

    ################
    # Adding stuff #
    ################

    def _add_attr(self, oname : str, a : Attr) -> None:
        '''Add to model'''
        self[oname].attrs[a.name]=a

    def _add_object(self, o : Obj) -> None:
        '''Add to model'''

        if o.name in self: # Validate
            raise ValueError('Cannot add %s, name is already taken!'%o)
        else:
            self.objs[o.name] = o # Add
            self._fks.add_node(o.name)


    def _add_view(self, v : View) -> None:
        '''Add to model'''
        # Validate
        #--------
        if v.name in self.views or v.name in self.objs:
            raise ValueError('Cannot add %s, name already taken!' % v)
        # Add
        #----
        self.views[v.name] = v

    def _add_relation(self, r : Rel) -> None:
        '''Add to model'''
        # Validate
        #---------
        err = 'Cannot add %s: %s not found in model '
        assert r.o1 in self, err%(r, r.o1)
        assert r.o2 in self, err%(r, r.o2)

        notfound = 'Cannot add %s, %s already has a %s with that name '

        for _,_,data in self._fks.edges(r.o1, data=True):
            if r.name in [fk.name for fk in data['fks']]:
                raise ValueError(notfound % (r, r.o1, 'relation'))
            if r.name in self[r.o1].attrnames():
                raise ValueError(notfound % (r, r.o1, 'attribute'))

        # Add
        #----
        self.add_fk(self._fks, r)

    def _add_patheq(self, peq : PathEQ) -> None:
        '''Add to model'''
        # Validate
        #---------
        rand  = next(iter(peq))
        start = rand.start()
        end   = rand._path_end(self)
        for p in peq:
            self._validate_path(p)
            assert p.start() == start
            assert p._path_end(self)   == end

        self.pe.add(peq)

    @staticmethod
    def add_fk(G : DiGraph, r : Rel, forward : bool = True) -> None:
        ''' Modify a graph (used by both add_relation and info_graph) '''
        a,b = (r.o1,r.o2)
        if not forward: a,b = b,a
        if G.has_edge(a, b):
            G[a][b]['fks'].add(r)
        else:
            G.add_edge(a, b, fks=set([r]))
    ###########
    # Objects #
    ###########
    def add_cols(self, obj : Obj) -> L[str]:
        attr_stmts = []
        for c in obj.attrs.values():
            obj_name = obj.name
            col_name, col_desc = c.create_col(obj.name)
            stmt = "ALTER TABLE %s ADD COLUMN %s"%(obj.name,col_name)
            attr_stmts.append(stmt)
            attr_stmts.append(col_desc)
        rel_stmts  = [self._create_fk(rel) for rel in self._obj_fks(obj.name)]
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
    ### FK Graph ###
    ################
    def _create_fk(self,fk:Rel)->str:
        '''create SQL FK statement'''
        args = [fk.o1,fk.name,fk.o2,self[fk.o2]._id]
        s ='''ALTER TABLE "{0}" ADD "{1}" INT;
           ALTER TABLE "{0}" ADD FOREIGN KEY ("{1}") REFERENCES "{2}"("{3}")'''
        return s.format(*args)

    def _rels(self) -> S[Rel]:
        ''' ALL relations between any objects, in some order '''
        fks = set()
        for _,_,d in self._fks.edges(data=True):
            for fk in d['fks']:
                fks.add(fk)
        return fks

    def _obj_all_fks(self, o : Obj) -> S[Rel]:
        ''' Relations that start OR end on a given object '''
        inward = set.union(*[d['fks'] for _,_,d in
                                        self._fks.in_edges(o.name,data=True)])
        return self._obj_fks(o.name) | inward

    def _obj_fks(self, o : str) -> S[Rel]:
        '''Relations that start from a given object'''
        sets = [d['fks'] for _,_,d in
                  self._fks.edges(o,data=True)]
        if sets:
            return set.union(*sets)
        else:
            return set()

    def get_rel(self, r : U[Rel,RelTup]) -> Rel:
        '''
        Upgrade a Relation representation that only has limited
        (but identifying) info
        '''
        if isinstance(r,Rel): return r

        for fk in self._obj_fks(r.obj):
            if fk.name == r.rel:
                return fk
        raise ValueError('Invalid RelTup %s'%r)

    def _info_graph(self, links : L[U[Rel,RelTup]]) -> DiGraph:
        '''Natural paths of information propagation, which includes the normal
            Rel relationships but also taking into account a
            user-specified list of relationships that are allowed to propagate
            information in the 'reverse' direction

            Furthermore, 1-1 relationships are identified and information is
            allowed to propagate in opposite direction there, too.
        '''
        G    = self._fks.copy()

        for name,o in self.objs.items():
            pars = [fk for fk in self._obj_fks(o.name) if fk.id]
            # only if it's a 1-1 table
            if len(pars)==1 and len(o.ids())==0:
                p   = pars[0] # identifying foreign key
                self.add_fk(G,p,forward=False)

        for fk in links:
            if isinstance(fk,RelTup):
                fk = self.get_rel(fk)
            self.add_fk(G,fk,forward=False)

        return G

    #########
    # Paths #
    #########
    def _validate_path(self, p : Path) -> None:
        '''Throw error if invalid path is passed'''
        curr = self[p.start()]
        for r in p.rels:
            rel = self.get_rel(r)
            assert rel.o1 == curr.name
            curr = self[rel.o2]
        if hasattr(p,'attr'):
            assert getattr(p,'attr').obj == curr.name

    #########
    # Views #
    #########
    def _show_view(self, vname : str) -> str:
        v = self.views[vname]
        if isinstance(v,RawView):
            return v.raw
        elif isinstance(v,QView):
            return v.q.showQ()
        else:
            raise TypeError
