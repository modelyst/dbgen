# External modules
from typing import (TYPE_CHECKING,
                     Any,
                     List     as L,
                     Dict     as D,
                     Union    as U,
                     Set      as S,
                     Tuple    as T)
from time            import time
from multiprocessing import cpu_count,get_context
from functools       import partial
from tqdm            import tqdm                                    # type: ignore
from networkx        import DiGraph               # type: ignore
from math            import ceil
import logging
# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    Model

from dbgen.core.schema          import Obj
from dbgen.core.misc            import ConnectInfo as ConnI
from dbgen.core.gen             import Gen
from dbgen.core.funclike        import PyBlock
from dbgen.core.action         import Action
from dbgen.core.misc            import ExternalError, SkipException

from dbgen.utils.lists          import broadcast, batch
from dbgen.utils.numeric        import safe_div
from dbgen.utils.sql            import (fast_load,sqlexecutemany, sqlexecute,sqlselect,mkSelectCmd,
                                    mkUpdateCmd,select_dict, Connection as Conn, DictCursor)
from dbgen.utils.str_utils      import hash_
###########################################

def run_gen(self            : 'Model',
            objs            : D[str,Any],
            gen             : Gen,
            gmcxn           : Conn,
            gcxn            : Conn,
            mconn_info      : ConnI,
            conn_info       : ConnI,
            run_id          : int,
            retry           : bool = False,
            serial          : bool = False,
            bar             : bool = False,
            user_batch_size : int  = None,
            skip_row_count  : bool = False,
            gen_hash        : int  = None,
           ) -> int:
    """
    Executes a SQL query, then maps each output over a processing function.
    How the DB is modified is determined self.actions (list of InsertUpdate).
    """
    # Initialize Variables
    #--------------------
    logger = logging.getLogger(f'run.{gen.name}')
    logger.setLevel(logging.DEBUG)

    gen.update_status(gmcxn,run_id,'running')
    start       = time()
    retry_      = retry or ('io' in gen.tags)
    if gen_hash:
        a_id = gen_hash
    else:
        a_id = gen.hash
    keys_to_save = gen._get_all_saved_key_dict()
    bargs        = dict(leave = False, position = 1, disable = not bar)
    bargs_inner  = dict(leave = False, position = 2, disable = not bar)
    parallel     = (not serial) and ('parallel' in gen.tags)

    # Set the hasher for checking repeats
    ghash        = gen.hash
    def hasher( x : Any) -> int:
        '''Unique hash function to this Generator'''
        return hash_(str(ghash) + str(x))

    # Determine how to map over input rows
    #-------------------------------------
    cxns      = (gmcxn,gcxn)

    cpus      = cpu_count()-1 or 1 # play safe, leave one free
    ctx       = get_context('forkserver') # addresses problem due to parallelization of numpy not playing with multiprocessing
    mapper    = partial(ctx.Pool(cpus).imap_unordered,chunksize = 5) if parallel else map

    # Wrap everything in try loop to catch errors
    try:
        # First setup query, get num_inputs and set batch size
        with tqdm(total=1, desc='Initializing Query', **bargs ) as tq:
            logger.debug('Initializing Query')
            # Name the cursor for server side processing, need to turn off auto_commit
            cursor      = conn_info.connect(auto_commit = False).cursor(f'{run_id}-{a_id}',cursor_factory = DictCursor)

            # If there is a query get the row count and execute it
            if gen.query:
                tq.set_description('Getting row count...')
                logger.info('Getting row count...')
                if not skip_row_count:
                    num_inputs = gen.query.get_row_count(gcxn)
                else:
                    num_inputs = int(10e8)
                logger.info(f'Number of inputs = {num_inputs}')
                logger.info('Executing query...')
                tq.set_description('Executing query...')
                cursor.execute(gen.query.showQ())
            # No query gens have 1 input
            else:
                num_inputs = 1

            # If user supplies a runtime batch_size it is used
            if user_batch_size is not None:
                batch_size = user_batch_size
                logger.info(f'Using user defined batch size: {user_batch_size}')
            # If generator has the batch_size set then that will be used next
            elif gen.batch_size is not None:
                batch_size = gen.batch_size
                logger.info(f'Using the {gen.name} specified batch size: {gen.batch_size}')
            # Finally if nothing is the default is set to batchify the inputs into
            # 20 batches
            else:
                batch_size = ceil(num_inputs/20) if num_inputs>0 else 1
                logger.info(f'Using the default batch size to get 20 batches: {batch_size}')


            tq.update()

        logger.info('Applying...')
        # Wrap batch processing in try loop to close the curosr on errors
        try:
            # Iterate over the batches
            for _ in tqdm(range(ceil(num_inputs/batch_size)), desc = 'Applying', **bargs):
                # fetch the current batch of inputs
                if gen.query:
                    logger.debug(f'Fetching batch')
                    inputs = cursor.fetchmany(batch_size)
                else:
                    # if there is no query set the inputs to be length 1 with
                    # empty dict as input
                    inputs = [{}]

                # If retry is true don't check for repeats
                if retry_:
                    inputs = [(x,hasher(x)) for x in inputs]
                # Check for repeats
                elif inputs:
                    with tqdm(total=1, desc='Repeat Checking', **bargs_inner ) as tq:
                        logger.debug(f'Repeat Checking')
                        # pair each input row with its hash
                        unfiltered_inputs = [(x,hasher(x)) for x in inputs]
                        # Get the already processed input hashes from the metadb
                        rpt_select        = 'SELECT repeats_id FROM repeats WHERE repeats.gen = %s'
                        rpt_select_output = sqlselect(gmcxn,rpt_select,[a_id])
                        rpt_select_output = [str(x[0]) for x in rpt_select_output] # type: ignore
                        rpt_select_output = set(rpt_select_output) # type: ignore
                        # remove the hashes that are already in the processed_hashes
                        inputs = [(x,hx) for x,hx in unfiltered_inputs if hx not in rpt_select_output] # type: ignore
                        tq.update()

                # If we have no query or if we have any inputs we apply the ETL to the batch
                if gen.query is None or len(inputs)>0:
                    apply_batch(inputs    = inputs,
                                f         = gen.funcs,
                                acts      = gen.actions,
                                objs      = objs,
                                a_id      = a_id,
                                qhsh      = gen.query.hash if gen.query else 0,
                                run_id    = run_id,
                                parallel  = parallel,
                                mapper    = mapper,
                                keys_to_save = keys_to_save,
                                cxns      = cxns,
                                gen_name = gen.name)
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
        logger.error(msg)
        q = mkUpdateCmd('gens',['error','status'],['run','name'])
        sqlexecute(gmcxn,q,[str(e),'failed',run_id,gen.name])
        return 1 # increment error count

def transform_func(input: T[dict,int],
                   qhsh : int,
                   pbs : L[PyBlock]
                  )->U[T[dict,int],T[None,None]]:
    row, hash = input
    try:
        d = {qhsh:row}
        for pb in pbs:
            d[pb.hash] = pb(d)
    except SkipException:
        return None, None
    return d, hash

def delete_unused_keys(namespace : D[int,Any],
                       keys_to_save : D[int,S[str]]
                      )->D[int,Any]:
    new_namespace = {}
    for hash_loc, names in keys_to_save.items():
        names_space_dict = namespace[hash_loc]
        pruned_dict = {key: val for key, val in names_space_dict.items() if key in names}
        new_namespace[hash_loc] = pruned_dict
    return new_namespace

def apply_batch(inputs        : L[T[dict,int]],
                 f            : L[PyBlock],
                 acts         : L[Action],
                 objs         : D[str,T[str,L[str],L[str]]],
                 a_id         : int,
                 run_id       : int,
                 qhsh         : int,
                 parallel     : bool,
                 mapper       : Any,
                 keys_to_save : D[int,S[str]],
                 cxns         : T[Conn, Conn],
                 gen_name         : str
                 )-> None:

    # Initialize variables
    logger               = logging.getLogger(f'run.{gen_name}.apply_batch')
    logger.setLevel(logging.DEBUG)

    open_mdb,open_db     = cxns
    n_inputs             = len(inputs)
    n_actions            = len(acts)
    processed_namespaces = [] # type: L[D[int,Any]]
    processed_hashes     = [] # type: L[int]
    skipped_rows         = 0
    bargs                = dict(leave = False, position = 2)

    logger.info('Transforming...')
    # Transform the data
    with tqdm(total=n_inputs, desc='Transforming', **bargs ) as tq:
        transform_func_curr = partial(transform_func,pbs = f, qhsh = qhsh)
        for i, (output_dict, output_hash) in enumerate(mapper(transform_func_curr, inputs)):
            if output_dict:
                processed_namespaces.append(delete_unused_keys(output_dict,keys_to_save))
                processed_hashes.append(output_hash)
            logger.debug(f'Transformed {i}/{n_inputs}')
            tq.update()

    logger.info('Loading...')
    # Load the data
    with tqdm(total=len(acts), desc='Loading', **bargs ) as tq:
        for i, a in enumerate(acts):
            a.act(cxn=open_db,objs=objs,rows=processed_namespaces, gen_name = gen_name)
            logger.debug(f'Loaded {i}/{n_actions}')
            tq.update()

    # Store the repeats
    logger.info('Storing Repeats')
    with tqdm(total=1, desc='Storing Repeats', **bargs ) as tq:
        repeat_values = broadcast([a_id,run_id,processed_hashes])
        table_name    = 'repeats'
        col_names     = ['gen','run','repeats_id']
        obj_pk_name   = 'repeats_id'
        fast_load(open_mdb, repeat_values, table_name, col_names, obj_pk_name)
