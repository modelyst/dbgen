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
from math            import ceil
# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    Model

from dbgen.core.schema          import Obj
from dbgen.core.misc            import ConnectInfo as ConnI
from dbgen.core.gen             import Gen
from dbgen.core.funclike        import PyBlock
from dbgen.core.action2          import Action
from dbgen.core.misc            import ExternalError, SkipException

from dbgen.utils.lists          import broadcast, batch
from dbgen.utils.numeric        import safe_div
from dbgen.utils.sql            import (fast_load,sqlexecutemany, sqlexecute,sqlselect,mkSelectCmd,
                                    mkUpdateCmd,select_dict, Connection as Conn, DictCursor)
from dbgen.utils.str_utils      import hash_
###########################################
rpt_select = """
    SELECT T.ind,T.repeats_id
    FROM temp T
        LEFT JOIN (SELECT repeats_id
                    FROM repeats
                    WHERE repeats.gen = %s) AS R
        USING (repeats_id)
    WHERE  R.repeats_id IS NULL;"""

def run_gen(self   : 'Model',
            gen             : Gen,
            gmcxn           : Conn,
            gcxn            : Conn,
            mconn           : ConnI,
            conn            : ConnI,
            run_id          : int,
            retry           : bool = False,
            serial          : bool = False,
            bar             : bool = False,
            user_batch_size : int = None
           ) -> int:
    """
    Executes a SQL query, then maps each output over a processing function.
    How the DB is modified is determined self.actions (list of InsertUpdate).
    """
    # Initialize Variables
    #--------------------
    start       = time()
    retry_      = retry or ('io' in gen.tags)
    a_id        = gen.get_id(gmcxn)[0][0]
    bargs       = dict(leave = False, position = 1, disable = not bar)
    bargs_inner = dict(leave = False, position = 2)
    parallel    = (not serial) and ('parallel' in gen.tags)


    # If user supplies a runtime batch_size it is used
    if user_batch_size is not None:
        batch_size = user_batch_size
    # If generator has the batch_size set then that will be used next
    elif gen.batch_size is not None:
        batch_size = gen.batch_size
    # Finally if nothing is the default of 100 is used
    else:
        batch_size = 100

    # Set the hasher for checking repeats
    ghash        = gen.hash
    def hasher( x : Any) -> int:
        '''Unique hash function to this Generator'''
        return hash_(str(ghash) + str(x))

    # Determine how to map over input rows
    #-------------------------------------
    cxns      = (gmcxn,gcxn)
    try:
        cursor      = conn.connect().cursor(cursor_factory = DictCursor)
        if gen.query:
            cursor.execute(gen.query.showQ())
            num_inputs = cursor.rowcount
        else:
            num_inputs = 1

        try:
            for _ in tqdm(range(ceil(num_inputs/batch_size)), desc = 'Applying', **bargs):
                if gen.query:
                    inputs = cursor.fetchmany(batch_size)
                else:
                    inputs = []

                if retry_:
                    inputs = [(x,hasher(x)) for x in inputs] # type: ignore
                elif inputs:
                    with tqdm(total=1, desc='Repeat Checking', **bargs_inner ) as tq:
                        unfiltered_inputs = [(x,hasher(x)) for x in inputs] # type: ignore
                        rpt_select = 'SELECT repeats_id FROM repeats WHERE repeats.gen = %s'
                        rpts = set([x[0] for x in sqlselect(gmcxn,rpt_select,[a_id])])
                        inputs = [(x,hx) for x,hx in unfiltered_inputs if hx not in rpts]
                        tq.update()

                if gen.query is None or len(inputs)>0:
                    apply_batch(inputs    = inputs,
                                f         = gen.funcs,
                                acts      = gen.actions,
                                objs      = self.objs,
                                a_id      = a_id,
                                qhsh      = gen.query.hash if gen.query else 0,
                                run_id    = run_id,
                                parallel  = parallel,
                                cxns      = cxns)
        finally:
            cursor.close()


        # Closing business
        #-----------------
        gen.update_status(gmcxn,run_id,'completed')
        tot_time = time() - start
        q = mkUpdateCmd('gens',['runtime','rate','n_inputs'],['run','name'])
        runtime = round(tot_time/60,4)
        rate    = round(safe_div(tot_time,num_inputs),4)
        sqlexecute(gmcxn,q,[runtime,rate,num_inputs,run_id,gen.name])
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
# ins_rpt_stmt = """ INSERT INTO repeats (gen,run,repeats_id) VALUES (%s,%s,%s)
#                     ON CONFLICT (repeats_id) DO NOTHING"""
#
# # Helper functions stored outside class so that they can be pickled by multiprocessing
# def apply_and_act(pbs    : L[PyBlock],
#                   acts   : L[Action],
#                   objs   : D[str,Obj],
#                   mcxn   : 'Conn',
#                   cxn    : 'Conn',
#                   row    : dict,
#                   hsh    : str,
#                   qhsh   : int,
#                   a_id   : int,
#                   run_id : int
#                  ) -> None:
#     """
#     The common part of parallel and serial application
#     """
#     d = {qhsh:row}
#     for pb in pbs:
#         d[pb.hash] = pb(d)
#
#     for a in acts:
#         a.act(cxn=cxn,objs=objs,rows=[d])
#
#     # If successful, store input+gen_id hash in metadb
#     sqlexecute(mcxn, ins_rpt_stmt, [a_id, run_id, hsh])
#
# def apply_parallel(inp   : T[dict, str, T['ConnI','ConnI']],
#                   f      : L[PyBlock],
#                   acts   : L[Action],
#                   objs   : D[str,Obj],
#                   a_id   : int,
#                   run_id : int,
#                   qhsh   : int
#                  ) -> None:
#
#     r,h,(mdb,db) = inp
#     open_db,open_mdb = db.connect(), mdb.connect()
#     apply_and_act(pbs = f, acts = acts, objs = objs,
#                   mcxn = open_mdb, cxn = open_db, row = r, qhsh = qhsh, hsh = h,
#                   a_id = a_id, run_id = run_id)
#     open_db.close(); open_mdb.close()
#
# def apply_serial(inp   : T[dict,str,T['ConnI','ConnI']],
#                  f      : L[PyBlock],
#                  acts   : L[Action],
#                  objs   : D[str,Obj],
#                  a_id   : int,
#                  run_id : int,
#                  qhsh   : int
#                  )-> None:
#     r,h,(open_mdb, open_db) = inp
#     apply_and_act(pbs = f, acts = acts, objs = objs,
#                   mcxn = open_mdb, cxn = open_db, row = r, hsh = h, qhsh = qhsh,
#                   a_id = a_id, run_id = run_id)
def transform_func(input: T[dict,int],
                   qhsh : int,
                   pbs : L[PyBlock]
                  )->T[dict,int]:
    row, hash = input
    try:
        d = {qhsh:row}
        for pb in pbs:
            d[pb.hash] = pb(d)
    except SkipException:
        print('Skipping row: {}'.format(row))
        return None, None
    return d, hash

def apply_batch(inputs     : L[T[dict,int]],
                 f      : L[PyBlock],
                 acts   : L[Action],
                 objs   : D[str,Obj],
                 a_id   : int,
                 run_id : int,
                 qhsh   : int,
                 parallel : bool,
                 cxns   : T[Conn, Conn]
                 )-> None:

    # Initialize variables
    open_mdb,open_db = cxns
    processed_namespaces = [] # type: L[D[int,Any]]
    processed_hashes     = [] # type: L[int]
    skipped_rows         = 0
    bargs                = dict(leave = False, position = 2)

    cpus      = cpu_count()-1 or 1 # play safe, leave one free
    ctx       = get_context('forkserver') # addresses problem due to parallelization of numpy not playing with multiprocessing
    mapper    = partial(ctx.Pool(cpus).imap_unordered,chunksize = 5) if parallel else map

    # Transform the data
    with tqdm(total=len(inputs), desc='Transforming', **bargs ) as tq:
        transform_func_curr = partial(transform_func,pbs = f, qhsh = qhsh)
        for output_dict, output_hash in mapper(transform_func_curr, inputs):
            if output_dict:
                processed_namespaces.append(output_dict)
                processed_hashes.append(output_hash)
            tq.update()

    # Load the data
    with tqdm(total=len(acts), desc='Loading', **bargs ) as tq:
        for a in acts:
            a.act(cxn=open_db,objs=objs,rows=processed_namespaces)
            tq.update()

    # Store the repeats
    with tqdm(total=1, desc='Storing Repeats', **bargs ) as tq:
        repeat_values = broadcast([a_id,run_id,processed_hashes])
        table_name    = 'repeats'
        col_names     = ['gen','run','repeats_id']
        obj_pk_name   = 'repeats_id'
        fast_load(open_mdb, repeat_values, table_name, col_names, obj_pk_name)
