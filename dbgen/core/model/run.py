from typing import TYPE_CHECKING, List as L
from copy   import deepcopy

from tqdm        import tqdm        # type: ignore

# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    Model
from dbgen.core.misc     import ConnectInfo as ConnI,Test,onlyTest,xTest
from dbgen.core.gen      import Gen
from dbgen.core.schema   import Path, PathEQ

from dbgen.utils.sql       import sqlexecute, sqlselect, OperationalError
from dbgen.utils.str_utils import levenshteinDistance
from dbgen.utils.lists     import concat_map
########################################################

def run(self      : 'Model',
        conn      : ConnI,
        meta_conn : ConnI,
        nuke      : bool = False,
        add       : bool = False,
        retry     : bool = False,
        only      : str = '',
        xclude    : str = '',
        start     : str = '',
        until     : str = '',
        serial    : bool = False,
        bar       : bool = True,
        clean       : bool = False
       ) -> None:

    todo = self._todo()
    if todo:
        print("WARNING: the following attributes do not have any generator "
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

    run_id = self._make_metatables(mconn=meta_conn,conn=conn,nuke=nuke,
                                  retry=retry,only=' '.join(sorted(only_)),
                                  xclude=' '.join(sorted(xclude_)),
                                  start=start,until=until,bar=bar)

    if nuke:
        self.make_schema(conn=conn,nuke=nuke,bar=bar)

    if add and not nuke:
        for ta in tqdm(self.objs.values(),desc='Adding new columns',leave=False):
            for sqlexpr in self.add_cols(ta):
                try:                     sqlexecute(conn.connect(),sqlexpr)
                except OperationalError: pass


    gcxn  = conn.connect()
    gmcxn = meta_conn.connect()

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

    with tqdm(total=len(self.gens), position = 0, disable = not bar) as tq:
        for gen in self.ordered_gens():

            name = gen.name
            tq.set_description(name)

            if name == start:
                start_flag = True

            if only: # only trumps everything else, if it's defined
                run = onlyTest(gen,only_) == True
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
                gen.update_status(gmcxn,run_id,'running')
                err_tot += self._run_gen(gen=gen,gmcxn=gmcxn,gcxn=gcxn,
                                      mconn=meta_conn,conn=conn,run_id=run_id,
                                      retry=retry,serial=serial,bar=bar)

            tq.update()

            if name == until:
                until_flag = False

    end = """UPDATE run SET delta=ROUND((CURRENT_TIMESTAMP()-starttime)/60.0,2)
                           ,errs = %s
             WHERE run_id=%s"""

    sqlexecute(gmcxn,end,[err_tot,run_id])

    self.check_paths(conn)

    if clean:
        for o in self.objs:
            for c in ['uid','deleted']:
                q = 'ALTER TABLE {} DROP COLUMN {}'.format(o,c)
                sqlexecute(gcxn,q)


    gcxn.close(); gmcxn.close()
    if bar:
        print('\nFinished.\n\t' +
              ('did not run %s'%not_run if not_run else 'Ran all Rules'))

def validate_name(self : 'Model', w : str) -> None:
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
    paths = list(p)
    ids   = {n:o._id for n,o in self.objs.items()}
    p1,p2 = paths
    sels  = p1.select(self),p2.select(self)
    joins = map('\n\t'.join,(p1.joins(ids,self),p2.joins(ids,self)))
    start = p1.start()
    st_id = self[start]._id

    q = '''
        SELECT `{0}`.`{1}`,
               {2},
               {3}
        FROM {0} AS `{0}`
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
