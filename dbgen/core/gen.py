from typing import (Any, TYPE_CHECKING,
                    List     as L,
                    Dict     as D,
                    Tuple    as T)

from networkx        import DiGraph # type: ignore
from jinja2          import Template
# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model


from dbgen.core.func     import Env, Import, defaultEnv
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
                 tags    : L[str]     = None,
                 env     : Env        = None,
                ) -> None:

        assert actions, 'Cannot have generator which does nothing'

        self.name    = name.lower()
        self.desc    = desc    or '<no description>'
        self.query   = query
        self.funcs   = self._order_funcs(funcs or [])
        self.actions = actions or []
        self.tags    = [t.lower() for t in tags or []]
        self.env     = env or defaultEnv
        for func in self.funcs:
            self.env += func.func.env

    def __str__(self) -> str:
        return 'Gen<%s>'%self.name

    ##################
    # Public Methods #
    ##################

    def update_status(self,conn:Conn,run_id:int,status:str)->None:
        q = mkUpdateCmd('gens',['status'],['run','name'])
        sqlexecute(conn,q,[status, run_id, self.name])

    def get_id(self, c : Conn) -> L[tuple]: # THIS IS OBSOLETE BC HASH IS ID?
        """ Assuming we've inserted already """
        check = self.hash
        get_a = mkSelectCmd('gen', ['gen_id'], ['gen_id'])
        return sqlselect(c, get_a, [check])

    def hasher(self, x : Any) -> int:
        '''Unique hash function to this Generator'''
        return hash_(str(self.hash) + str(x))

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
            cmd  = mkInsCmd('gen', ['gen_id', 'name', 'description'])
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
        d = {str(pb.hash) : pb for pb in pbs}
        G.add_nodes_from(d.keys())
        for pb in pbs:
            for a in pb.args:
                if isinstance(a, Arg) and a.key != 'query':
                    assert a.key in d, pb.func.name
                    G.add_edge(a.key, str(pb.hash))
        return topsort_with_dict(G, d)

    # ######################
    # # Airflow Operator Exports
    # # --------------------
    def operator(self) -> str:
        '''Real simple thing to get UI up and running'''
        return 'print("{}")'.format(self.name)

    def test_operator(self, model_name : str, universe : D[str,Obj])-> str:

        template = '''

##########################
# Initialize environment #
##########################

from airflow.hooks.postgres_hook import PostgresHook # type: ignore
conn    = PostgresHook('{{ model_name }}').get_conn()
cursor  = conn.cursor()
mconn   = PostgresHook('{{ model_name }}_log').get_conn()
mcursor = mconn.cursor()


##############################
# Initialization for EXTRACT #
##############################

{% if query %}
query = """ {{ query }} """
{% endif %}

################################
# Initialization for TRANSFORM #
################################

{{ env }}

{% for pyblock_name,_,pyblock_def in pyblocks %}
def {{ pyblock_name }}(**namespace : Any) -> dict:
    {{ pyblock_def }}
{% endfor %}

# (These have been ordered deliberately)
pyblocks = [{% for pyblock_name,pyblock_hash,_ in pyblocks %} ({{ pyblock_hash }},{{ pyblock_name }}), {% endfor %}] # type: list

def TRANSFORM_{{ name }}(namespace : dict) -> dict:
    for pyblock_hash, pyblock_func in pyblocks:
        namespace[pyblock_hash] = pyblock_func(**namespace)
    return namespace

###########################
# Initialization for LOAD #
###########################

from hashlib import sha256

def hash_(x : Any)->int:
    return (int(sha256(str(x).encode('utf-8')).hexdigest(), 16) % 18446744073709551616) - 9223372036854775808

def broadcast(args : list) -> L[list]:
    broad_err   = "Can't broadcast: maxlen = %d, len a = %d (%s)"
    maxlen = 1 # initialize variable
    for a in args:
        if isinstance(a,(list,tuple)):
            if maxlen != 1: assert(len(a) in [1,maxlen]), broad_err%(maxlen,len(a),str(a))
            else:   maxlen = len(a) # set variable for first (and last) time
    def process_arg(x:Any)->list:
        if isinstance(x,(list,tuple)) and len(x)!=maxlen:     return maxlen*list(x) # broadcast
        elif not isinstance(x,list): return  maxlen * [x]
        else: return x
    return list(zip(*[process_arg(x) for x in args]))

def addQs(xs:list,delim:str)->str: return delim.join(['{0} = %s'.format(x) for x in xs])

def sqlexecute(q : str, binds : list = [], meta : bool = False) -> list:
    cur = mcursor if meta else cursor
    cur.execute(q,vars=binds)
    if 'select' or 'returning' in q.lower():
        result = cur.fetchall()
    else:
        result = []
    conn.commit()
    return result


objs = dict({% for obj,(pk,ids,idfks) in objs.items() %} {{ obj }} = ('{{ pk }}', {{ ids }},{{ idfks }}),\n{% endfor %}) # type: D[str,T[str,L[str],L[str]]]

class Arg(object):
    def __init__(self, key : str, name  : str) -> None: self.key = key; self.name=name
    def arg_get(self, dic : dict) -> Any: return dic[self.key][self.name]
class Const(object):
    def __init__(self,val:Any) -> None: self.val = val
    def arg_get(self, _ : dict) -> Any: return self.val

class Load(object):
    def __init__(self, obj: str, attrs : dict,fks : dict,pk : Any, insert : bool) -> None:
        self.obj=obj;self.attrs=attrs;self.fks=fks;self.pk=pk;self.insert=insert
    def act(self, row : dict) -> None:
        self._insert(row) if self.insert else self._update(row)
    def _getvals(self,row  : dict,) -> T[L[int],L[list]]:
        idattr,allattr = [],[]
        pk,ids,id_fks = objs[self.obj]
        for k,v in self.attrs.items():
            val = v.arg_get(row)
            allattr.append(val)
            if k in ids: idattr.append(val)
        for kk,vv in self.fks.items():
            if vv.insert: val = vv._insert(row)
            else: assert vv.pk; val = vv.pk.arg_get(row)
            allattr.append(val)
            if kk in id_fks: idattr.append(val)
        idata,adata = broadcast(idattr),broadcast(allattr)
        if self.pk is not None:
            assert not idata, 'Cannot provide a PK *and* identifying info'
            pkdata = self.pk.arg_get(row)
            if isinstance(pkdata,int):                                  idata = [[pkdata]]
            elif isinstance(pkdata,list) and isinstance(pkdata[0],int): idata = [pkdata]
            else: raise TypeError('PK should either receive an int or a list of ints',vars(self))

        if len(idata) == 1: idata*= len(adata) # broadcast
        lenerr = 'Cannot match IDs to data: %d!=%d'
        assert len(idata) == len(adata), lenerr%(len(idata),len(adata))
        return list(map(hash_,idata)), adata
    def _insert(self,row  : dict, ) -> L[int]:
        objpk,_,_ = objs[self.obj]
        pk,data = self._getvals(row)
        if not data: return []
        binds = [list(x)+[u]+list(x) for x,u in zip(data,pk)] # double the binds
        cols        = list(self.attrs.keys()) + list(self.fks.keys()) + [objpk]
        insert_cols = ','.join(['"%s"'%x for x in cols])
        qmarks      = ','.join(['%s']*len(cols))
        dups        = addQs(['"%s"'%x for x in cols[:-1]],', ')
        fmt_args    = [self.obj, insert_cols, qmarks, dups, objpk]
        query       = """INSERT INTO {0} ({1}) VALUES ({2}) ON CONFLICT ({4}) DO UPDATE SET {3} RETURNING {4}""".format(*fmt_args)
        ids = [sqlexecute(query,b)[0][0] for b in binds]
        return ids
    def _update(self, row  : dict ) -> list:
        objpk,_,_ = objs[self.obj]
        pk,data = self._getvals(row)
        if not data: return []
        binds = [list(x)+[u] for x,u in zip(data,pk)]
        cols  = list(self.attrs.keys()) + list(self.fks.keys())
        set_  = addQs(['"%s"'%x for x in cols],',')
        query = 'UPDATE {0} SET {1} WHERE {2} = %s'.format(self.obj,set_,objpk)

        for b in binds: sqlexecute(query,b)
        return []

loaders = [{% for load in loads %} {{ load }},{% endfor %}] # type: L[Load]

def LOAD_{{ name }}(namespace : dict) -> None:
    for loader in loaders: loader.act(namespace)
    conn.close()

#############
# Execution #
#############

{% if query  %}

with conn.cursor(name = '{{ name }}', cursor_factory = DictCursor) as cxn:
    cxn.execute(query)
    for row in cxn:
        # To Do: repeat checking

        # Do T & L on a per-row basis:
        LOAD_{{ name }}(TRANSFORM_{{ name }}(dict(query = row)))

{% else %}

LOAD_{{ name }}(TRANSFORM_{{ name }}(dict(query = dict())))

{% endif %}
        '''
        pbs = [('pb'+str(pb.hash).replace('-','neg'),pb.hash,pb.make_src()) for pb in self.funcs]
        loaders = [loader.make_src() for loader in self.actions]
        objs = {oname : (o._id, repr(o.ids()),repr(o.id_fks())) for oname, o in universe.items()}

        kwargs = dict(name = self.name, pyblocks = pbs,
            env = str(self.env) if self.env else '',loads=loaders, objs=objs,
            query = self.query.showQ() if self.query else False,
            model_name=model_name)
        return Template(template).render(**kwargs)
