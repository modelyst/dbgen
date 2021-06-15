#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import logging
import re
from bdb import BdbQuit
from typing import TYPE_CHECKING
from typing import List as L

from tqdm import tqdm

from dbgen.core.gen import Generator

# Internal imports
from dbgen.core.misc import ConnectInfo as ConnI
from dbgen.core.misc import Test, onlyTest, xTest
from dbgen.core.schema import PathEQ
from dbgen.utils.lists import concat_map
from dbgen.utils.sql import Error, sqlexecute, sqlselect
from dbgen.utils.str_utils import levenshteinDistance

# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model

    Model

########################################################


def run(
    self: "Model",
    conn: ConnI,
    meta_conn: ConnI,
    nuke: bool = False,
    add: bool = False,
    retry: bool = False,
    only: L[str] = [],
    xclude: L[str] = [],
    start: str = None,
    until: str = None,
    serial: bool = False,
    bar: bool = True,
    clean: bool = False,
    skip_row_count: bool = False,
    batch: int = None,
    print_logo: bool = True,
) -> None:
    """
    This method is point of the model: to run and generate a database according
    to the model's specified rules.

    - conn/meta_conn: information to connect to database and logging database
    - nuke: By default, this is not used. If True, everything except generators
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

    """
    # Run tests on pyblocks
    # ----------------------
    self.test_transforms()

    # # Setup logger and config
    # # --------------------
    run_logger = logging.getLogger("dbgen.run")
    # Print to-do list for the model
    # ---------------------------------------
    todo = self._todo()
    if todo:
        run_logger.warning(
            "the following attributes do not have any generator "
            "to populate them: \n\t-" + "\n\t-".join(sorted(todo))
        )

    # Validate input
    # ----------------
    assert conn != meta_conn, "Main DB cannot be in same schema as logging DB"
    startErr = 'Starting generator ("start") must be a Generator name'
    assert not start or start in self.gens, startErr
    xclude_ = set(xclude)
    only_ = set(only)
    for w in only_ | xclude_:
        self._validate_name(w)

    # # Make sure no existing cxns to database
    # # ---------------------------------------
    # conn.kill()
    # meta_conn.kill()

    # Make metatables
    # ----------------
    run_id = self._make_metatables(
        mconn=meta_conn,
        conn=conn,
        nuke=nuke,
        retry=retry,
        only=list(sorted(only_)),
        xclude=list(sorted(xclude_)),
        start=start,
        until=until,
        bar=bar,
    )

    # Clean up database
    # -----------------
    if nuke:
        self.make_schema(conn=conn, nuke=nuke, bar=bar)
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
        run_logger.warning(msg)
        for ta in tqdm(
            self.objs.values(),
            desc="Adding new tables",
            leave=False,
            disable=not bar,
        ):
            for sqlexpr in ta.create():
                try:
                    sqlexecute(conn.connect(), sqlexpr)
                except Error as e:
                    # Error code for duplicate table
                    if e.pgcode == "42701":
                        run_logger.debug("dup")
                        pass
                    # Error code for when a relation doesn't exist on a table we
                    # are adding
                    elif re.match(r'column "\w+" of relation "\w+" does not exist', str(e)):
                        run_logger.debug(f"PGERROR: {e}")
                        pass
                    else:
                        raise Error(e)
        for v in tqdm(self.viewlist, desc="Adding new views", leave=False, disable=not bar):
            try:
                sqlexecute(conn.connect(), v.create())
            except Error as e:
                # Error code for duplicate table
                if "already exists" in str(e):
                    run_logger.debug("dup")
                    pass
                else:
                    raise Error(e)

        for ta in tqdm(
            self.objs.values(),
            desc="Adding new columns",
            leave=False,
            disable=not bar,
        ):
            for sqlexpr in self.add_cols(ta):
                try:
                    sqlexecute(conn.connect(), sqlexpr)
                except Error as e:
                    # Error code for duplicate column
                    if e.pgcode == "42701":
                        pass
                    else:
                        raise Error(e)

    # Make 'global' database connections (active throughout whole process)
    # ----------------------------------------------------------------------
    gcxn = conn.connect()
    gmcxn = meta_conn.connect()

    # Initialize variables
    # ---------------------
    not_run = []  # type: L[str] ### List of Rules that were not run
    err_tot = 0  # total # of failed generators
    start_flag = False if start else True
    until_flag = True
    start_test = Test(lambda _, __: start_flag, lambda _: 'excluded')
    until_test = Test(lambda _, __: until_flag, lambda _: 'excluded')
    testdict = {xTest: [xclude_], start_test: [None], until_test: [None]}

    universe = self._get_universe()

    def update_run_status(status: str) -> None:
        update_run_status = """UPDATE run SET status=%s WHERE run_id=%s"""
        sqlexecute(gmcxn, update_run_status, [status, run_id])

    # Set status to running
    update_run_status("running")
    with tqdm(total=len(self.gens), position=0, disable=not bar) as tq:
        for gen in self.ordered_gens():

            # Initialize Variables
            # ---------------------
            name = gen.name
            tq.set_description(name)
            run_logger.info(f"Running {gen.name}...")

            # Set flags
            # --------------------------------
            if name == start:
                start_flag = True

            # Run tests to see whether or not the Generator should be run
            if only:  # only trumps everything else, if it's defined
                only_result = onlyTest(gen, only_)
                xclude_result = xTest(gen, xclude_)
                run = (only_result is True) and (xclude_result is True)
                if only_result is not True:
                    gen.update_status(gmcxn, run_id, only_result)
                elif xclude_result is not True:
                    gen.update_status(gmcxn, run_id, xclude_result)
            else:
                run = True  # flag for passing all tests
                for test, args in testdict.items():
                    test_output = test(gen, *args)  # type: ignore
                    if test_output is not True:
                        not_run.append(name)
                        gen.update_status(gmcxn, run_id, test_output)
                        run = False
                        break
            if run:
                try:
                    err_tot += self._run_gen(
                        universe=universe,
                        gen=gen,
                        gmcxn=gmcxn,
                        gcxn=gcxn,
                        mconn_info=meta_conn,
                        conn_info=conn,
                        run_id=run_id,
                        retry=retry,
                        serial=serial,
                        bar=bar,
                        user_batch_size=batch,
                        skip_row_count=skip_row_count,
                    )
                except (
                    Exception,
                    KeyboardInterrupt,
                    SystemExit,
                    BdbQuit,
                ) as exc:
                    # If a critical error is hit that doesn't raise
                    # ExternalError() we need to clean up
                    # Update the run
                    update_run_status("failed")
                    # Update the Gen
                    error = str(exc) if str(exc) else repr(exc)
                    gen.update_status(gmcxn, run_id, "failed", err=error)
                    raise

            tq.update()

            # Set flags
            # ----------
            if name == until:
                until_flag = False

    end = """UPDATE run SET delta=EXTRACT(EPOCH FROM age(CURRENT_TIMESTAMP,starttime)),
                            status='completed',
                            errs = %s
             WHERE run_id=%s"""

    sqlexecute(gmcxn, end, [err_tot, run_id])
    self.check_paths(conn)

    if clean:
        for o in self.objs:
            for c in ["deleted"]:
                q = f"ALTER TABLE {o} DROP COLUMN {c}"
                sqlexecute(gcxn, q)

    gcxn.close()
    gmcxn.close()
    if bar:
        run_logger.info("\nFinished.\n\t" + (f"did not run {not_run}" if not_run else "Ran all Rules"))


def validate_name(self: "Model", w: str) -> None:
    """
    Checks to make sure name - in an argument of model.run() - is valid,
    If not, throws error and suggests alternatives
    """
    match = False
    close = []

    def t(u: Generator) -> L[str]:
        return [u.name] + u.tags

    for n in concat_map(t, list(self.gens.values())):
        d = levenshteinDistance(w, n)
        upW, upN = max(len(w), 5), max(len(n), 5)  # variables for safe indexing
        if d == 0:
            match = True
            break
        elif d < 5 or w[:upW] == n[:upN]:
            close.append(n)  # keep track of near-misses
    if not match:
        did_you = f"Did you mean {close}" if close else ""
        raise ValueError(f"No match found for {w}\n{did_you}")


def check_patheq(self: "Model", p: PathEQ, db: ConnI) -> None:
    """
    Check whether a given database enforces the a path equality specification
    """
    paths = list(p)
    ids = {n: o.id_str for n, o in self.objs.items()}
    p1, p2 = paths
    sels = p1.select(self), p2.select(self)
    joins = map("\n\t".join, (p1.joins(ids, self), p2.joins(ids, self)))
    start = p1.start()
    st_id = self[start].id_str

    q = """
        SELECT "{0}"."{1}",
               {2},
               {3}
        FROM {0} AS "{0}"
        {4}
        {5}
        """
    args = [start, st_id, *sels, *joins]
    query = q.format(*args)
    out = sqlselect(db.connect(), query)
    for id, a, b in out:
        if a != b:
            err = "Path Equality check FAILED for {} # {}" + "\n{} -> {}" * 2
            eargs = (start, id, p1, a, p2, b)
            raise ValueError(err.format(*eargs))
