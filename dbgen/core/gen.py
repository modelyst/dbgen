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

################################################################################

class Gen(Base):
    def __init__(self,
                 name    : str,
                 desc    : str        = None,
                 query   : Query      = None,
                 funcs   : L[PyBlock] = None,
                 actions : L[Action]  = None,
                 tags    : L[str]     = None
                ) -> None:

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
        if self.query:
            tabdeps = self.query.allobj()
            coldeps = ['%s.%s'%(a.obj,a.name) for a in self.query.allattr()]
            for r in self.query.allrels():
                coldeps.append(r.obj+ '.' + r.rel)
        else:
            tabdeps,coldeps = [],[]

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
        a_id = self.get_id(cxn)
        if a_id:
            return a_id[0][0]
        else:
            cmd  = mkInsCmd('gen', ['uid', 'name', 'description'])
            sqlexecute(cxn, cmd,[self.hash, self.name, self.desc])
            aid = self.get_id(cxn)
            return aid[0][0]

    def rename_object(self,o : Obj, n :str) -> 'Gen':
        g  = self.copy()
        if g.query:
            g.query.basis = [n if b == o.name else b for b in g.query.basis]
        for i,a in enumerate(g.actions):
            g.actions[i] = a.rename_object(o,n)



        return g

    ##################
    # Private Methods #
    ##################
    @staticmethod
    def _order_funcs(pbs : L[PyBlock]) -> L[PyBlock]:
        G = DiGraph()
        d = {pb.hash : pb for pb in pbs}
        G.add_nodes_from(d.keys())
        for pb in pbs:
            for a in pb.args:
                if isinstance(a, Arg) and a.key != 'query':
                    assert a.key in d, pb.func.name
                    G.add_edge(a.key, pb.hash)
        return topsort_with_dict(G, d)
