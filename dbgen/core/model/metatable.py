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

from dbgen.core.sqltypes   import Varchar, Decimal, Text, Timestamp, Int, Boolean
from dbgen.core.schema     import Obj, Rel, Attr
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
        attrs = [Attr('hostname',   Varchar(),id=True),
                  Attr('user',      Varchar(),id=True),
                  Attr('port',                id=True),
                  Attr('db',        Varchar(),id=True)]),
    Obj('temp',
        desc = 'Temporary table that is populated and truncated after checking for repeat values',
        attrs = [Attr('ind',id=True,desc='Index to a list of query-generated inputs')]),

    Obj('object','All static info about a given class of entities being modeled',
        attrs=[Attr('name',         Varchar(),id=True),
               Attr('description',  Text())]),

    Obj('attr','Property of an object',
        attrs=[Attr('name',         Varchar(),id=True),
               Attr('dtype',        Varchar()),
               Attr('description',  Text()),
               Attr('defaultval',   Text())]),

    Obj('view','SQL view',
        attrs = [Attr('name',Varchar(),id=True),
                 Attr('query',Text('long'))]),

    Obj('func','Python functions that get used during generation of Objects/Attributes',
        attrs=[Attr('source',   Text(),id=True),
               Attr('name',     Varchar())]),

    Obj('gen','Method for generating concrete data',
        attrs=[Attr('name',         Varchar(),id=True),
               Attr('description',  Text())]),

    Obj('pyblock','decorated python function',attrs=[]),

    Obj('const','A constant injected into the namespace of an generator',
        attrs=[Attr('dtype',Varchar(),id=True),
               Attr('val',  Text(),   id=True)]),

    Obj('arg','How a PyBlock refers to a namespace',
        attrs=[Attr('ind',      Int(),id=True),
               Attr('keyname',  Varchar()),
               Attr('name',     Varchar())]),

    Obj('run','Each time DbGen is run, a new Run instance is created',
        attrs= [Attr('starttime',Timestamp(),default='CURRENT_TIMESTAMP'),
                Attr('start',   Varchar()),
                Attr('until_',  Varchar()),
                Attr('delta',   Decimal(),desc='Runtime in minutes'),
                Attr('errs',    Int()),
                Attr('retry',   Boolean()),
                Attr('nuke', Varchar()),
                Attr('onlyrun', Varchar()),
                Attr('exclude', Varchar())]),

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
                Attr('basis',       Varchar())]),

    Obj('objs','A list of Object instances associated with a given run'),
    Obj('views','List of View instances associated with a given run'),
    Obj('repeats','A record of which inputs a given Action has already seen'),
        ]

rels = [Rel('object',   'attr',    id = True),
        Rel('gen',      'pyblock', id = True),
        Rel('func',     'pyblock'),
        Rel('connection','run'),
        Rel('const',     'arg'),
        Rel('run',      'gens',id=True),
        Rel('gen',      'gens',id=True),
        Rel('object',   'objs',id=True),
        Rel('run',      'objs',id=True),
        Rel('gen',      'repeats',id=True),
        Rel('run',      'repeats'),
        Rel('view',     'views',id=True),
        Rel('run',      'views',id=True)
        ]

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
    meta.add(objs); meta.add(rels)

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
    run_id = sqlselect(gmcxn,'SELECT MAX(run_id)+1 FROM run')[0][0] or 1

    # Insert connection (if it dosn't exist already)
    #----------------------------------------------
    cxn_cols  = ['uid','hostname','user','port','db']
    cxn_args_ = [conn.host,conn.user,conn.port,conn.db]
    cxn_args  = [hash_('$'.join(map(str,cxn_args_)))] + cxn_args_ # type: ignore
    cxn_sql   = mkInsCmd('connection',cxn_cols)
    sqlexecute(gmcxn,cxn_sql,cxn_args)

    # Get current connection ID
    #----------------------------------------------
    get_cxn = mkSelectCmd('connection',['connection_id'],['uid'])
    cxn_id  = sqlselect(gmcxn,get_cxn,cxn_args[:1])[0][0]

    # Insert top level information about current run
    #----------------------------------------------
    run_cols = ['run_id','uid','retry','onlyrun','exclude','nuke',
                'connection','start','until_']

    run_args = [run_id, run_id, retry, only, xclude, nuke, cxn_id, start, until]

    fmt_args = [','.join(run_cols), ','.join(['%s'] * len(run_args))]
    run_sql  = 'INSERT INTO run ({}) VALUES ({})'.format(*fmt_args)
    sqlexecute(gmcxn,run_sql,run_args)

    # Insert info about current DBG if it doesn't exist
    #----------------------------------------------
    od = 'Inserting Objects into MetaDB'
    ad = 'Inserting Actions into MetaDB'
    vd = 'Inserting Views into MetaDB'
    oq = mkInsCmd('objs',['run','object','uid'])
    vq = mkInsCmd('views',['run','view','uid'])
    aq = mkInsCmd('gens',['run','gen','name','status','description',
                             'query','ind','tabdep','coldep','newtab','newcol',
                             'basis','uid'])
    tqargs = dict(leave=False, disable = not bar)
    for o in tqdm(self.objs.values(),desc=od,**tqargs):
        safex(gmcxn,oq,[run_id,o.add(gmcxn),hash_(str(run_id)+o.hash)])

    for vn,v in tqdm(self.views.items(),desc=vd,**tqargs):
        safex(gmcxn,vq,[run_id,v.add(gmcxn),hash_(str(run_id)+v.hash)])

    for i,u in enumerate(tqdm(self.ordered_gens(),desc=ad,**tqargs)):
        #print(i,u)
        v = u.query
        q = v.showQ() if v else ''
        td,cd,nt,nc = u.dep().all()
        b = ','.join(v.basis) if v else ''

        safex(gmcxn,aq,[run_id,u.add(gmcxn),u.name,'initialized',u.desc,q,
                             i,td,cd,nt,nc,b,hash_(str(run_id)+u.hash)])
    return run_id
