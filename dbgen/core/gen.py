from typing import (Any, TYPE_CHECKING,
                    List     as L,
                    Tuple    as T)

from networkx        import DiGraph # type: ignore

# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    T

from dbgen.core.func     import Env, Import
from dbgen.core.funclike import PyBlock, Arg
from dbgen.core.action   import Action
from dbgen.core.query    import Query
from dbgen.core.misc     import Dep
from dbgen.core.schema   import Obj

from dbgen.utils.graphs    import topsort_with_dict
from dbgen.utils.misc      import Base
from dbgen.utils.str_utils import hash_
from dbgen.utils.sql       import (Connection as Conn,sqlexecute,mkSelectCmd,
                                   mkUpdateCmd,sqlselect, mkInsCmd)

'''
Defines a Generator, as well as a Model method that is directly related
'''
################################################################################

class Gen(Base):
    '''Generator: populates database with data'''

    def __init__(self,
                 name    : str,
                 desc    : str        = None,
                 query   : Query      = None,
                 funcs   : L[PyBlock] = None,
                 actions : L[Action]  = None,
                 tags    : L[str]     = None
                ) -> None:

        assert actions, 'Cannot have generator which does nothing'

        self.name    = name.lower()
        self.desc    = desc    or '<no description>'
        self.query   = query
        self.funcs   = self._order_funcs(funcs or [])
        self.actions = actions or []
        self.tags    = [t.lower() for t in tags or []]

    def __str__(self) -> str:
        return 'Gen<%s>'%self.name

    ##################
    # Public Methods #
    ##################

    def update_status(self,conn:Conn,run_id:int,status:str)->None:
        q = mkUpdateCmd('gens',['status'],['run','name'])
        sqlexecute(conn,q,[status, run_id, self.name])

    def get_id(self, c : Conn) -> L[tuple]:
        """ Assuming we've inserted already """
        check = self.hash
        get_a = mkSelectCmd('gen', ['gen_id'], ['uid'])
        return sqlselect(c, get_a, [check])

    def hasher(self, x : Any) -> str:
        '''Unique hash function to this Generator'''
        return hash_(self.hash + str(x))

    def dep(self) -> Dep:
        '''
        Determine the tabs/cols that are both inputs and outputs to the Gen
        '''
        # Analyze allattr and allobj to get query dependencies
        if self.query:
            tabdeps = self.query.allobj()
            coldeps = ['%s.%s'%(a.obj,a.name) for a in self.query.allattr()]
            for r in self.query.allrels():
                coldeps.append(r.obj+ '.' + r.rel)
        else:
            tabdeps,coldeps = [],[]

        # Analyze actions to see what new cols and tabs are yielded
        newtabs,newcols = [], [] # type: T[L[str],L[str]]

        for a in self.actions:
            newtabs.extend(a.newtabs())
            newcols.extend(a.newcols())

        # Allow for unethical hacks
        for t in self.tags:
            if t[:4] == 'dep ':
                coldeps.append(t[4:])

        return Dep(tabdeps,coldeps,newtabs,newcols)

    def add(self, cxn : 'Conn') -> int:
        '''
        Add the Generator to the metaDB which stores info about a model (if
        it's not already in there) and return the ID
        '''
        a_id = self.get_id(cxn)
        if a_id:
            return a_id[0][0]
        else:
            cmd  = mkInsCmd('gen', ['uid', 'name', 'description'])
            sqlexecute(cxn, cmd,[self.hash, self.name, self.desc])
            aid = self.get_id(cxn)
            return aid[0][0]

    def rename_object(self,o : Obj, n :str) -> 'Gen':
        '''Change all references to an object to account for name change'''
        g  = self.copy()
        if g.query:
            g.query.basis = [n if b == o.name else b for b in g.query.basis]
        for i,a in enumerate(g.actions):
            g.actions[i] = a.rename_object(o,n)
        return g

    def purge(self, conn : Conn, mconn : Conn) -> None:
        '''
        If a generator is purged, then any
        tables it populates will be truncated. Any columns it populates will be set all
        to NULL'''
        d = self.dep()
        tabs,cols = d.tabs_yielded,d.cols_yielded
        for t in tabs:
            sqlexecute(conn,'TRUNCATE {} CASCADE'.format(t))

        for t,c in map(lambda x: x.split('.'),cols):
            sqlexecute(mconn,'UPDATE {} SET {} = NULL'.format(t,c))

        gids = sqlselect(mconn,'SELECT gen_id FROM gen WHERE name = %s',[self.name])
        for gid in gids:
            sqlexecute(mconn,"DELETE FROM repeats WHERE gen_id = %s",[gid])

    ##################
    # Private Methods #
    ##################
    @staticmethod
    def _order_funcs(pbs : L[PyBlock]) -> L[PyBlock]:
        '''Make dependency graph among PyBlocks and determine execution order'''
        G = DiGraph()
        d = {pb.hash : pb for pb in pbs}
        G.add_nodes_from(d.keys())
        for pb in pbs:
            for a in pb.args:
                if isinstance(a, Arg) and a.key != 'query':
                    assert a.key in d, pb.func.name
                    G.add_edge(a.key, pb.hash)
        return topsort_with_dict(G, d)

# def from_sqlite(self : "Model", pth : str) -> Gen:
#     '''Given an SQLite instance that matches the model, a Generator can be
#         defined that populates the *entire* model from that file'''
#     from sqlite3 import connect # type: ignore
#     e = Env(Import('sqlite3','connect'))
#     conn = connect(pth)
#     for obj in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
#         pass
#     def f(sqlpth : str, objname : str, attrs : L[str]) -> tuple:
#         conn = connect(pth)
#
#         outputs = [] # type: list
#         return tuple(outputs)
#         raise NotImplementedError
#
#     outnames = ['']
#     pb       = PyBlock(f,e,outnames = outnames)
#     actions  = [o.default_action(pb) for o in self.objs.values()]
#     return Gen(name = 'fromsqlite',
#                desc ='Populates from '+pth,
#                funcs = [pb],
#                actions = actions)
