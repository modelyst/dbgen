from typing import TYPE_CHECKING, List as L
from copy   import deepcopy

from tqdm        import tqdm        # type: ignore
import logging

# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    Model
from dbgen.core.misc     import ConnectInfo as ConnI,Test,onlyTest,xTest
from dbgen.core.gen      import Gen
from dbgen.core.schema   import Path, PathEQ

from dbgen.utils.sql       import sqlexecute, sqlselect, Error
from dbgen.utils.str_utils import levenshteinDistance
from dbgen.utils.lists     import concat_map
from dbgen.utils.logging import setup_logger
########################################################


def run(self      : 'Model',
        conn           : ConnI,
        meta_conn      : ConnI,
        nuke           : str = '',
        add            : bool = False,
        retry          : bool = False,
        only           : str = '',
        xclude         : str = '',
        start          : str = '',
        until          : str = '',
        serial         : bool = False,
        bar            : bool = True,
        clean          : bool = False,
        skip_row_count : bool = False,
        batch          : int = None,
        log_level      : int = logging.INFO
       ) -> None:
    '''
    This method is point of the model: to run and generate a database according
    to the model's specified rules.

    - conn/meta_conn: information to connect to database and logging database
    - nuke: By default, this is not used. If "True"/"T", everything except generators
            tagged "no_nuke" are purged. Otherwise, give a space separated list
            of generator names/tags. If a generator is purged, then any
            tables it populates will be truncated. Any columns it populates will be set all
            to NULL. Any generators with inputs OR outputs that have any overlap with the outputs
            of a purged generator will be purged themselves.
    - add: needed if new entities/columns have been added to the model (but not yet in DB)
    - retry: ignore repeat checking
    - only: only run generators with these names (or these tags)
    - xclude: do not run generators with these names (or these tags)
    - start: start at the generator with this name
    - until: stop at the generator with this name
    - serial: force all Generators to be run without parallelization
    - bar: show progress bars
    - skip_row_count: Skip the select count(1) for all gens (good for large queries)
    - clean: 'cleans up' implementation detail columns (deleted) for
    presentation of the resulting database to others...at the cost of not being
    able to call model.run() without nuking again (unless an 'unclean' method is
    written, which is in principle possible)

    '''


    # Run tests on pyblocks
    #----------------------
    self.test_funcs()


    # # Setup logger
    # # --------------------
    logger = setup_logger('run', log_level)

    # Print to-do list for the model
    #---------------------------------------
    todo = self._todo()
    if todo:
        logger.warning("WARNING: the following attributes do not have any generator "
              "to populate them: \n\t-"+'\n\t-'.join(sorted(todo)))

    # Validate input
    #----------------
    assert conn != meta_conn, 'Main DB cannot be in same schema as logging DB'
    startErr = 'Starting generator ("start") must be a Generator name'
    assert not start or start in self.gens, startErr
    xclude_ = set(xclude.split())
    only_   = set(only.split())
    for w in (only_ | xclude_):
        self._validate_name(w)


    # Make sure no existing cxns to database
    #---------------------------------------
    conn.kill(); meta_conn.kill()

    # Make metatables
    #----------------
    run_id = self._make_metatables(mconn=meta_conn,conn=conn,nuke=nuke,
                                  retry=retry,only=' '.join(sorted(only_)),
                                  xclude=' '.join(sorted(xclude_)),
                                  start=start,until=until,bar=bar)

    # Clean up database
    #-----------------
    if nuke:
        if nuke.lower() in ['t','true']:
            self.make_schema(conn=conn,nuke=nuke,bar=bar) # FULL NUKE
        else:
            raise NotImplementedError('Selective nuking is not working yet')
            deltags = set(nuke.split())
            delgens = set()
            for gen in self.ordered_gens():
                if set([gen.name]+gen.tags).intersection(deltags):
                    delgens.add(gen.name)
            for gen in self.ordered_gens():
                if gen.name in delgens:
                    gen.purge(conn.connect(),meta_conn.connect())
    elif add:
        msg = """
#######################################################################
!!!WARNING!!!!
Add is an extremely experimental feature. Existing rows in
modified tables are not deleted. If you added ID info to a table then
you need to manually truncate that table (and cascade to linked tables)
or else those tables will contain rows with missing ID info in their PKs.

Add should really only be used to add attributes to existing tables
or add new empty tables. Adding identifying FKs from existing tables to new tables
is very dangerous and manual truncation will be necessary.

I hope you know what you are doing!!!
!!!WARNING!!!!
#######################################################################
        """
        logger.warning(msg)
        for ta in tqdm(self.objs.values(), desc='Adding new tables', leave=False):
            if not getattr(ta,'_is_view',False):
                for sqlexpr in ta.create():
                    try:
                        sqlexecute(conn.connect(),sqlexpr)
                    except Error as e:
                        # Error code for duplicate table
                        if e.pgcode == '42701':
                            logger.debug('dup')
                            pass
                        else:
                            raise Error(e)

        for ta in tqdm(self.objs.values(),desc='Adding new columns', leave=False):
            for sqlexpr in self.add_cols(ta):
                try:
                    sqlexecute(conn.connect(),sqlexpr)
                except Error as e:
                    # Error code for duplicate column
                    if e.pgcode == '42701':
                        pass
                    else:
                        raise Error(e)


    # Make 'global' database connections (active throughout whole process)
    #----------------------------------------------------------------------
    gcxn  = conn.connect()
    gmcxn = meta_conn.connect()

    # Initialize variables
    #---------------------
    not_run    = []    # type: L[str] ### List of Rules that were not run
    err_tot    = 0     # total # of failed generators
    start_flag = False if start else True
    until_flag = True
    start_test = Test(lambda _,__: start_flag,
                      lambda _:'Excluded because of "start"')
    until_test = Test(lambda _,__: until_flag,
                      lambda _:'Excluded because of "until"')
    testdict = {xTest      : [xclude_],
                start_test : [None],
                until_test : [None]}

    objs            = {oname   : (o._id, o.ids(), o.id_fks()) for oname, o in self.objs.items()}

    with tqdm(total=len(self.gens), position = 0, disable = not bar) as tq:
        for gen in self.ordered_gens():

            # Initialize Variables
            #---------------------
            name = gen.name
            tq.set_description(name)
            logger.info(f'Running {gen.name}...')

            # Set flags
            #--------------------------------
            if name == start:
                start_flag = True

            # Run tests to see whether or not the Generator should be run
            if only: # only trumps everything else, if it's defined
                run = (onlyTest(gen,only_) is True) and (xTest(gen,xclude_) is True)
            else:
                run = True  # flag for passing all tests
                for test,args in testdict.items():
                    test_output = test(gen,*args) # type: ignore
                    if test_output is not True:
                        not_run.append(name)
                        gen.update_status(gmcxn,run_id,test_output)
                        run = False
                        break
            if run:
                err_tot += self._run_gen(
                    objs            = objs,
                    gen             = gen,
                    gmcxn           = gmcxn,
                    gcxn            = gcxn,
                    mconn_info      = meta_conn,
                    conn_info       = conn,
                    run_id          = run_id,
                    retry           = retry,
                    serial          = serial,
                    bar             = bar,
                    user_batch_size = batch,
                    skip_row_count  = skip_row_count
                )

            tq.update()

            # Set flags
            #----------
            if name == until:
                until_flag = False

    end = """UPDATE run SET delta=EXTRACT(EPOCH FROM age(CURRENT_TIMESTAMP,starttime))
                           ,errs = %s
             WHERE run_id=%s"""

    sqlexecute(gmcxn,end,[err_tot,run_id])

    self.check_paths(conn)

    if clean:
        for o in self.objs:
            for c in ['deleted']:
                q = 'ALTER TABLE {} DROP COLUMN {}'.format(o,c)
                sqlexecute(gcxn,q)


    gcxn.close(); gmcxn.close()
    if bar:
        logger.info('\nFinished.\n\t' +
              ('did not run %s'%not_run if not_run else 'Ran all Rules'))

def validate_name(self : 'Model', w : str) -> None:
    """
    Checks to make sure name - in an argument of model.run() - is valid,
    If not, throws error and suggests alternatives
    """
    match = False
    close = []
    def t(u : Gen) -> L[str]: return [u.name]+u.tags
    for n in concat_map(t,list(self.gens.values())):
        d = levenshteinDistance(w,n)
        upW,upN = max(len(w),5), max(len(n),5) # variables for safe indexing
        if d == 0:
            match = True
            break
        elif d < 5 or w[:upW] == n[:upN]:
            close.append(n) # keep track of near-misses
    if not match:
        did_you = "Did you mean %s"%close if close else ''
        raise ValueError("No match found for %s\n%s"%(w,did_you))


def check_patheq(self : 'Model', p : PathEQ, db : ConnI) -> None:
    '''
    Check whether a given database enforces the a path equality specification
    '''
    paths = list(p)
    ids   = {n:o._id for n,o in self.objs.items()}
    p1,p2 = paths
    sels  = p1.select(self),p2.select(self)
    joins = map('\n\t'.join,(p1.joins(ids,self),p2.joins(ids,self)))
    start = p1.start()
    st_id = self[start]._id

    q = '''
        SELECT "{0}"."{1}",
               {2},
               {3}
        FROM {0} AS "{0}"
        {4}
        {5}
        '''
    args  = [start,st_id,*sels,*joins]
    query = q.format(*args)
    out   = sqlselect(db.connect(),query)
    for id,a,b in out:
        if a!=b:
            err = 'Path Equality check FAILED for {} # {}'\
                   + '\n{} -> {}'*2
            eargs = (start,id,p1,a,p2,b)
            raise ValueError(err.format(*eargs))
