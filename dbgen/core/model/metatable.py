# External Modules
from typing  import TYPE_CHECKING, Any
from tqdm    import tqdm                       # type: ignore
from copy    import deepcopy

# Internal Modules
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    from dbgen.core.misc        import ConnectInfo as ConnI
    from dbgen.core.gen         import Gen
    ConnI,Model,Gen

from dbgen.core.expr.sqltypes   import Varchar, Decimal, Text, Timestamp, Int, Boolean
from dbgen.core.schema     import Obj, UserRel as Rel, Attr
from dbgen.utils.str_utils import hash_
from dbgen.utils.sql       import mkInsCmd, sqlexecute, mkSelectCmd, sqlselect
#############################################################################
def safex(conn:Any,q:str,binds:list)->None:
    try:
        sqlexecute(conn,q,binds)
    except Exception as e:
        import pdb;pdb.set_trace()

###########
# Constants
##########
create_curr_run = '''
CREATE OR REPLACE VIEW curr_run AS
    SELECT name,status,runtime,n_inputs,rate,error,query,description,
           tabdep,coldep,newtab,newcol,basis
    FROM gens
    WHERE gens.run = (SELECT max(run.run_id) FROM run)
    ORDER BY gens.ind
'''
objs = [
    Obj('connection',"Info required to connect to a PostGres DB",
        attrs = [Attr('hostname',   Varchar(),identifying=True),
                  Attr('user',      Varchar(),identifying=True),
                  Attr('port',                identifying=True),
                  Attr('db',        Varchar(),identifying=True)]),
    Obj('temp',
        desc = 'Temporary table that is populated and truncated after checking for repeat values',
        attrs = [Attr('ind',identifying=True,desc='Index to a list of query-generated inputs')]),

    Obj('object','All static info about a given class of entities being modeled',
        attrs=[Attr('name',         Varchar(),identifying=True),
               Attr('description',  Text())],
        ),

    Obj('attr','Property of an object',
        attrs=[Attr('name',         Varchar(),identifying=True),
               Attr('dtype',        Varchar()),
               Attr('description',  Text()),
               Attr('defaultval',   Text())],
        fks = [Rel('object', identifying= True)]),

    Obj('view','SQL view',
        attrs = [Attr('name',Varchar(),identifying=True),
                 Attr('query',Text('long'))]),

    Obj('func','Python functions that get used during generation of Objects/Attributes',
        attrs=[Attr('source',   Text(),identifying=True),
               Attr('name',     Varchar())]),

    Obj('gen','Method for generating concrete data',
        attrs=[Attr('name',         Varchar(),identifying=True),
               Attr('description',  Text()),
               Attr('gen_json',  Text())]),

    Obj('pyblock','decorated python function',attrs=[],
        fks = [Rel('gen',  identifying= True),Rel('func')]),

    Obj('const','A constant injected into the namespace of an generator',
        attrs=[Attr('dtype',Varchar(),identifying=True),
               Attr('val',  Text(),   identifying=True)]),

    Obj('arg','How a PyBlock refers to a namespace',
        attrs=[Attr('ind',      Int(),identifying=True),
               Attr('keyname',  Varchar()),
               Attr('name',     Varchar())],
        fks = [Rel('const')]),

    Obj('run','Each time DbGen is run, a new Run instance is created',
        attrs= [Attr('starttime',Timestamp(),default='CURRENT_TIMESTAMP'),
                Attr('start',   Varchar()),
                Attr('until_',  Varchar()),
                Attr('delta',   Decimal(),desc='Runtime in minutes'),
                Attr('errs',    Int()),
                Attr('retry',   Boolean()),
                Attr('nuke', Varchar()),
                Attr('onlyrun', Varchar()),
                Attr('exclude', Varchar())],
        fks = [Rel('connection')]),

    Obj('gens','A list of Generator instances associated with a given run',
        attrs =[Attr('name',        Varchar()),
                Attr('status',      Varchar()),
                Attr('runtime',     Decimal()),
                Attr('n_inputs',    Int()),
                Attr('rate',        Decimal()),
                Attr('error',       Text('long')),
                Attr('description', Text()),
                Attr('query',       Text()),
                Attr('ind',         Int()),
                Attr('tabdep',      Text()),
                Attr('coldep',      Text()),
                Attr('newtab',      Text()),
                Attr('newcol',      Text()),
                Attr('basis',       Varchar())],
        fks = [Rel('run',identifying=True),Rel('gen',identifying=True)]),

    Obj('objs','A list of Object instances associated with a given run',
        fks = [Rel('object',identifying=True),Rel('run',identifying=True),]),
    Obj('views','List of View instances associated with a given run',
        fks = [Rel('view',identifying=True),Rel('run', identifying=True)]),
    Obj('repeats','A record of which inputs a given Action has already seen',
        fks = [Rel('gen',identifying=True),Rel('run' ),]),]




#############################################################################
# Main function

def make_meta(self   : 'Model',
              mconn  : 'ConnI',
              conn   : 'ConnI',
              nuke   : str,
              retry  : bool,
              only   : str,
              xclude : str,
              start  : str,
              until  : str,
              bar    : bool,
             ) -> int:
    """
    Initialize metatables
    """

    NUKE_META = True # whether or not to erase metatable data if nuking DB
    meta = self._build_new('meta')
    meta.add(objs);

    ################################################################################

    if nuke.lower() in ['t','true'] and NUKE_META:
        mconn.drop(); mconn.create()
        gmcxn = mconn.connect()
    else:
        try:
            gmcxn = mconn.connect()
        except:
            raise Exception('When making DB for first time, run with --nuke=True')

    # Create metatables if they don't exist
    #--------------------------------------

    for t in meta.objs.values():
        for sql in t.create(): sqlexecute(gmcxn,sql)

    if nuke:
        for r in meta._rels():
            sqlexecute(gmcxn,meta._create_fk(r))

    sqlexecute(gmcxn,create_curr_run)

    # Create new run instance
    #-------------------------
    run_id= sqlselect(gmcxn,'SELECT MAX(run_id)+1 FROM run')[0][0] or 1

    # Insert connection (if it dosn't exist already)
    #----------------------------------------------
    cxn_cols  = ['connection_id','hostname','user','port','db']
    cxn_args_ = [conn.host,conn.user,conn.port,conn.db]
    cxn_args  = [hash_('$'.join(map(str,cxn_args_)))] + cxn_args_ # type: ignore
    cxn_sql   = mkInsCmd('connection',cxn_cols)
    sqlexecute(gmcxn,cxn_sql,cxn_args)

    # Get current connection ID
    #----------------------------------------------
    get_cxn = mkSelectCmd('connection',['connection_id'],['connection_id'])
    cxn_id = sqlselect(gmcxn,get_cxn,cxn_args[:1])[0][0]

    # Insert top level information about current run
    #----------------------------------------------
    run_cols = ['run_id','retry','onlyrun','exclude','nuke',
                'connection','start','until_']

    run_args = [run_id, retry, only, xclude, nuke, cxn_id, start, until]

    fmt_args = [','.join(run_cols), ','.join(['%s'] * len(run_args))]
    run_sql  = 'INSERT INTO run ({}) VALUES ({})'.format(*fmt_args)

    sqlexecute(gmcxn,run_sql,run_args)

    # Insert info about current DBG if it doesn't exist
    #----------------------------------------------
    od = 'Inserting Objects into MetaDB'
    ad = 'Inserting Actions into MetaDB'
    vd = 'Inserting Views into MetaDB'
    oq = mkInsCmd('objs',['run','object','objs_id'])
    vq = mkInsCmd('views',['run','view','views_id'])
    aq = mkInsCmd('gens',['run','gen','name','status','description',
                             'query','ind','tabdep','coldep','newtab','newcol',
                             'basis','gens_id'])
    tqargs = dict(leave=False, disable = not bar)
    for o in tqdm(self.objs.values(),desc=od,**tqargs):
        safex(gmcxn,oq,[run_id,o.add(gmcxn),hash_(str(run_id)+str(o.hash))])

    for vn,v in tqdm(self.views.items(),desc=vd,**tqargs):
        safex(gmcxn,vq,[run_id,v.add(gmcxn),hash_(str(run_id)+str(v.hash))])

    for i,u in enumerate(tqdm(self.ordered_gens(),desc=ad,**tqargs)):
        #print(i,u)
        v = u.query
        q = v.showQ() if v else ''
        td,cd,nt,nc = u.dep(self.objs).all()
        b = ','.join(v.basis) if v else ''

        safex(gmcxn,aq,[run_id,u.add(gmcxn),u.name,'initialized',u.desc,q,
                             i,td,cd,nt,nc,b,hash_(str(run_id)+str(u.hash))])
    return run_id
