# External modules
from typing import (TYPE_CHECKING,
                     Any,
                     List     as L,
                     Dict     as D,
                     Tuple    as T)

from time            import time
from multiprocessing import cpu_count,get_context
from functools       import partial
from tqdm            import tqdm                                    # type: ignore
from networkx        import DiGraph               # type: ignore

# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    Model

from dbgen.core.schema          import Obj
from dbgen.core.misc            import ConnectInfo as ConnI
from dbgen.core.gen             import Gen
from dbgen.core.funclike        import PyBlock
from dbgen.core.action          import Action
from dbgen.core.misc            import ExternalError

from dbgen.utils.numeric        import safe_div
from dbgen.utils.sql            import (sqlexecute,sqlselect,mkSelectCmd,
                                    mkUpdateCmd,select_dict, Connection as Conn)
from dbgen.utils.str_utils      import hash_
###########################################
rpt_select = """
    SELECT T.ind,T.uid
    FROM temp T
        LEFT JOIN (SELECT uid
                    FROM repeats
                    WHERE repeats.gen = %s) AS R
        USING (uid)
    WHERE  R.uid IS NULL;"""

def run_gen(self   : 'Model',
            gen    : Gen,
            gmcxn  : Conn,
            gcxn   : Conn,
            mconn  : ConnI,
            conn   : ConnI,
            run_id : int,
            retry  : bool = False,
            serial : bool = False,
            bar    : bool = False
           ) -> int:
    """
    Executes a SQL query, then maps each output over a processing function.
    How the DB is modified is determined self.actions (list of InsertUpdate).
    """
    # Initialize Variables
    #--------------------
    start        = time()
    retry_       = retry or ('io' in gen.tags)
    a_id         = gen.get_id(gmcxn)[0][0]
    bargs        = dict(leave = False, position = 1, disable = not bar)
    parallel     = (not serial) and ('parallel' in gen.tags)
    ghash        = gen.hash

    def hasher( x : Any) -> str:
        '''Unique hash function to this Generator'''
        return hash_(ghash + str(x))

    # Determine how to map over input rows
    #-------------------------------------
    cpus      = cpu_count()-1 or 1 # play safe, leave one free
    cxns      = (mconn,conn)            if parallel else (gmcxn,gcxn)
    ctx       = get_context('forkserver') # addresses problem due to parallelization of numpy not playing with multiprocessing
    mapper    = partial(ctx.Pool(cpus).imap_unordered,chunksize = 5) if parallel else map

    applyfunc = apply_parallel if parallel else apply_serial

    try:
        if 'stream' in gen.tags:
            assert gen.query
            cxn = conn.connect().cursor()
            cxn.execute(gen.query.showQ())

            for row in tqdm(cxn, position = 1, desc = 'stream', **bargs):
                if retry_: # skip repeat checking; external world changed
                    is_rpt = [] # type: list
                else:
                    inshash = hasher(row)
                    rptq    = mkSelectCmd('repeats',['gen'],['gen','uid'])
                    is_rpt  = sqlselect(gmcxn, rptq, [a_id, inshash])
                if len(is_rpt) == 0:
                    d = {} # type: dict
                    for pb in gen.funcs:
                        d[pb.hash] = pb(d)
                    for a in gen.actions:
                        a.act(cxn=gcxn,objs=self.objs,fks=self._fks,row=d)
            cxn.close()

        else:
            if not gen.query:
                inputs = [{}] # type: ignore
            else:
                with tqdm(total=1,desc='querying',**bargs) as tq:
                    q      = gen.query.showQ()
                    try:
                        inputs = select_dict(gcxn,q)
                    except Exception as e:
                        print(e);print(q);import pdb;pdb.set_trace();assert False
                    tq.update()

            if len(inputs)>0:
                if retry_: # skip repeat checking
                    inputs = [(x,hasher(x),cxns) for x in inputs] # type: ignore
                else:
                    with tqdm(total=1,desc='repeat_checking',**bargs) as tq:
                        unfiltered_inputs = [(x,hasher(x)) for x in inputs] # type: ignore
                        rpt_select = 'SELECT uid FROM repeats WHERE repeats.gen = %s'
                        rpts = set([x[0] for x in sqlselect(gmcxn,rpt_select,[a_id])])
                        inputs = [(x,hx,cxns) for x,hx in unfiltered_inputs if hx not in rpts]
                        tq.set_description('repeat_checking (selecting non-repeats)')
                        tq.update()

            tot = len(inputs)
            with tqdm(total=tot, desc='applying', **bargs) as tq:
                f = partial(applyfunc,
                            f      = gen.funcs,
                            acts   = gen.actions,
                            objs   = self.objs,
                            fks    = self._fks,
                            a_id   = a_id,
                            run_id = run_id)

                for _ in mapper(f, inputs): # type: ignore
                    tq.update()

        # Closing business
        #-----------------
        gen.update_status(gmcxn,run_id,'completed')
        tot_time = time() - start
        q = mkUpdateCmd('gens',['runtime','rate','n_inputs'],['run','name'])
        rate    = round(tot_time/60,4)
        runtime = round(safe_div(tot_time,tot),2)
        sqlexecute(gmcxn,q,[runtime,rate,tot,run_id,gen.name])
        return 0 # don't change error count

    except ExternalError as e:
        msg = '\n\nError when running generator %s\n'%gen.name
        print(msg)
        q = mkUpdateCmd('gens',['error','status'],['run','name'])
        sqlexecute(gmcxn,q,[str(e),'failed',run_id,gen.name])
        return 1 # increment error count

#############
# CONSTANTS #
#############
ins_rpt_stmt = """ INSERT INTO repeats (gen,run,uid) VALUES (%s,%s,%s)
                    ON CONFLICT (uid) DO NOTHING"""

# Helper functions stored outside class so that they can be pickled by multiprocessing
def apply_and_act(pbs    : L[PyBlock],
                  acts   : L[Action],
                  objs   : D[str,Obj],
                  fks    : DiGraph,
                  mcxn   : 'Conn',
                  cxn    : 'Conn',
                  row    : dict,
                  hsh    : str,
                  a_id   : int,
                  run_id : int
                 ) -> None:
    """
    The common part of parallel and serial application
    """
    d = {'query':row}
    for pb in pbs:
        d[pb.hash] = pb(d)

    for a in acts:
        a.act(cxn=cxn,objs=objs,fks=fks,row=d)

    # If successful, store input+gen_id hash in metadb
    sqlexecute(mcxn, ins_rpt_stmt, [a_id, run_id, hsh])

def apply_parallel(inp   : T[dict, str, T['ConnI','ConnI']],
                  f      : L[PyBlock],
                  acts   : L[Action],
                  objs   : D[str,Obj],
                  fks    : DiGraph,
                  a_id   : int,
                  run_id : int
                 ) -> None:

    r,h,(mdb,db) = inp
    open_db,open_mdb = db.connect(), mdb.connect()
    apply_and_act(pbs = f, acts = acts, objs = objs, fks = fks,
                  mcxn = open_mdb, cxn = open_db, row = r, hsh = h,
                  a_id = a_id, run_id = run_id)
    open_db.close(); open_mdb.close()

def apply_serial(inp   : T[dict,str,T['ConnI','ConnI']],
                 f      : L[PyBlock],
                 acts   : L[Action],
                 objs   : D[str,Obj],
                 fks    : DiGraph,
                 a_id   : int,
                 run_id : int
                 )-> None:
    r,h,(open_mdb, open_db) = inp
    apply_and_act(pbs = f, acts = acts, objs = objs, fks = fks,
                  mcxn = open_mdb, cxn = open_db, row = r, hsh = h,
                  a_id = a_id, run_id = run_id)
