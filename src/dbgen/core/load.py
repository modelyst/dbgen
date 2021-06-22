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

"""Define the Load Object for inserting transformed data into the Database."""
import logging
import re
from io import StringIO
from random import getrandbits

# External Modules
from typing import TYPE_CHECKING
from typing import Dict as D
from typing import List as L
from typing import Tuple as T
from typing import Union as U

import psycopg2
from hypothesis.strategies import SearchStrategy, builds, dictionaries, just, one_of
from jinja2 import Template
from psycopg2.errors import QueryCanceled

from dbgen.core.funclike import Arg, ArgLike, Const
from dbgen.utils.exceptions import DBgenExternalError, DBgenTypeError, Psycopg2Error
from dbgen.utils.lists import broadcast
from dbgen.utils.misc import Base, nonempty
from dbgen.utils.sql import Connection as Conn
from dbgen.utils.str_utils import hashdata_

# Internal Modules
if TYPE_CHECKING:
    from dbgen.core.model.model import UNIVERSE_TYPE
    from dbgen.core.schema import Entity

"""
Defines the class of modifications to a database

There is a horrific amount of duplicated code in this file...... oughta fixit
"""
################################################################################
# ######################
# # Constants
# # --------------------
NUM_QUERY_TRIES = 3


class Load(Base):
    """
    The purpose for this object is to make an easily serializable data structure
    that knows how to update the database (these methods could easily be for
    Model, but we don't want to send the entire model just to do this small thing)
    """

    def __init__(
        self,
        obj: str,
        attrs: D[str, ArgLike],
        fks: D[str, "Load"],
        pk: U[Arg, Const] = None,
        insert: bool = False,
        partition_attr: str = None,
    ) -> None:
        """
        Initializes Load object with relevant Objects and nested Loads. This is
        not intended to be called by users, but rather called through the
        Entity.__call__ method (Entity()).

        Args:
            obj (str): [description]
            attrs (D[str, ArgLike]): [description]
            fks (D[str,): [description]
            pk (Arg, optional): [description]. Defaults to None.
            insert (bool, optional): [description]. Defaults to False.
        """
        self.obj = obj.lower()
        self.attrs = {k.lower(): v for k, v in attrs.items()}
        self.fks = {k.lower(): v for k, v in fks.items()}
        self.pk = pk
        self.insert = insert
        self.partition_attr = partition_attr
        self._logger = logging.getLogger(f"dbgen.run.loading.{self.obj}")
        self._logger.setLevel(logging.DEBUG)
        err = "Cant insert %s if we already have PK %s"
        assert (pk is None) or (not insert), err % (obj, pk)
        if isinstance(pk, Const) and pk == Const(None):
            pass
        else:
            assert isinstance(pk, (Arg, type(None))), (
                obj,
                attrs,
                fks,
                pk,
                insert,
            )
        super().__init__()

    def __str__(self) -> str:
        n = len(self.attrs)
        m = len(self.fks)
        return "Load<%s, %d attr, %d rel>" % (self.obj, n, m)

    ##################
    # Public methods #
    ###################
    def tabdeps(self, universe: D[str, "Entity"]) -> L[str]:
        """All tables that are updated (they must already exist, is the logic)"""
        from dbgen.core.schema import Partition

        # Check if we are updating; if so we depend on
        entity = universe[self.obj]
        out = [entity.name] if not self.insert else []

        # Check for partitions on self table
        # If so add partitions if Entity and add parent entity if partition
        if out:
            if isinstance(entity, Partition):
                out.append(entity._parent_name)
            elif entity.partition_attr:
                partition_names = map(lambda x: x.name, entity.get_all_partitions())
                out.extend(partition_names)

        for fk in self.fks.values():
            # Check if we are simply setting the FK to null
            if not isinstance(fk.pk, Const) or fk.pk != Const(None):
                out.extend(fk.tabdeps(universe))
        return out

    def newtabs(self, universe: D[str, "Entity"]) -> L[str]:
        """All tables that could be inserted into this load"""
        from dbgen.core.schema import Partition

        entity = universe[self.obj]
        out = [entity.name] if self.insert else []

        # Check for partitions on self table
        # If so add partitions if Entity and add parent entity if partition
        if out and isinstance(entity, Partition):
            out.append(entity._parent_name)
        elif out and entity.partition_attr:
            partition_names = map(lambda x: x.name, entity.get_all_partitions())
            out.extend(partition_names)

        for a in self.fks.values():
            out.extend(a.newtabs(universe))
        return out

    def newcols(self, universe: D[str, "Entity"]) -> L[str]:
        """All attributes that could be populated by this load"""
        from dbgen.core.schema import Partition

        entity = universe[self.obj]
        if isinstance(entity, Partition):
            new_entities = [entity, universe[entity._parent_name]]
        else:
            new_entities = [entity, *entity.get_all_partitions()]
        out: L[str] = []
        for new_entity in new_entities:
            out.extend(
                [
                    new_entity.name + "." + a
                    for a in self.attrs.keys()
                    if (self.insert or (a not in new_entity.ids()))
                ]
            )
            for k, a in self.fks.items():
                if self.insert or (k not in new_entity.id_fks()):
                    out.extend([new_entity.name + "." + k] + a.newcols(universe))
        return out

    def act(
        self,
        cxn: Conn,
        universe: "UNIVERSE_TYPE",
        rows: L[dict],
        gen_name: str,
    ) -> None:
        """
        Top level call from a Generator to execute an load (top level is
        always insert or update, never just a select)
        """
        # Initialize logger
        self._load(cxn, universe, rows, insert=self.insert)

    def rename_object(self, o: "Entity", n: str) -> "Load":
        """Replaces all references to a given object to one having a new name"""
        a = self.copy()
        if a.obj == o.name:
            a.obj = n
        for k, v in a.fks.items():
            a.fks[k] = v.rename_object(o, n)
        return a

    @classmethod
    def _strat(cls) -> SearchStrategy:
        """A hypothesis strategy for generating random examples."""

        common_load_kwargs = dict(
            obj=nonempty,
            attrs=dictionaries(keys=nonempty, values=ArgLike._strat()),
        )
        load_ = builds(
            cls,
            fks=just(dict()),
            pk=Arg._strat(),
            insert=just(False),
            **common_load_kwargs,  # type: ignore
        )
        load0 = builds(
            cls,
            fks=dictionaries(keys=nonempty, values=load_),
            pk=Arg._strat(),
            insert=just(False),
            **common_load_kwargs,  # type: ignore
        )
        load1 = builds(
            cls,
            fks=dictionaries(keys=nonempty, values=load_),
            pk=just(None),
            insert=just(True),
            **common_load_kwargs,  # type: ignore
        )
        return one_of(load_, load0, load1)

    ###################
    # Private methods #
    ###################

    def _getvals(
        self,
        universe: "UNIVERSE_TYPE",
        row: dict,
    ) -> T[L[int], L[tuple]]:
        """
        Get a broadcasted list of INSERT/UPDATE values for an object, given
        Pyblock+Query output
        """

        idattr, allattr = [], []
        obj_pk_name, ids, id_fks, dtype_dict = universe[self.obj]
        for k, v in sorted(
            self.attrs.items(),
        ):
            val = v.arg_get(row)
            dtype = dtype_dict[k]
            if isinstance(val, (list, tuple)):
                func = lambda x: [dtype.cast(ele) for ele in x]
            else:
                func = dtype.cast
            try:
                allattr.append(func(val))
            except DBgenTypeError:
                self._logger.error(
                    f"While processing a row we encountered a value that violated column {k}'s type constraint {dtype}."
                    f"\nRow: {row}\nVal: {val}"
                )
                raise
            if k in ids:
                idattr.append(allattr[-1])

        for kk, vv in sorted(self.fks.items()):
            if vv.pk is not None:
                val = vv.pk.arg_get(row)
                if isinstance(val, int) or val is None:
                    pass
                elif isinstance(val, list) and isinstance(val[0], int):
                    pass
                else:
                    raise ValueError(f"Primary Key is not an int or None: {row}\n{vv}\n{vv.pk}\n{val}")
            else:
                val, fk_adata = vv._getvals(universe, row)

            allattr.append(val)
            if kk in id_fks:
                idattr.append(val)

        idata: L[tuple] = broadcast(idattr)
        adata: L[tuple] = broadcast(allattr)
        if self.pk is not None:
            assert not idata, "Cannot provide a PK *and* identifying info"
            pkdata = self.pk.arg_get(row)
            if isinstance(pkdata, int):
                idata_prime = [pkdata]
            elif isinstance(pkdata, list) and isinstance(pkdata[0], int):  # HACKY
                idata_prime = pkdata
            elif isinstance(pkdata, str) or (isinstance(pkdata, list) and isinstance(pkdata[0], str)):
                raise TypeError(
                    f"While looking for the PK on {self.obj}, we found a string or list of strings: {pkdata}\n"
                    "PK's should be integers for hashing purposes."
                )
            else:
                raise TypeError(
                    "PK should either receive an int or a list of ints",
                    vars(self),
                )
        else:
            idata_prime = []
            idata_dict = {}  # type: D[tuple,int]
            # Iterate through the identifying data and cache the hashed result for speed
            for idata_curr in idata:
                if idata_dict.get(idata_curr) is None:
                    try:
                        idata_dict[idata_curr] = int(hashdata_(idata_curr))
                    except TypeError:
                        print(idata_curr)
                idata_prime.append(idata_dict[idata_curr])

        if len(idata_prime) == 1:
            idata_prime *= len(adata)  # broadcast

        lenerr = "Cannot match IDs to data: %d!=%d"
        assert len(idata_prime) == len(adata), lenerr % (
            len(idata_prime),
            len(adata),
        )
        return idata_prime, adata

    def _data_to_stringIO(
        self,
        pk: L[int],
        data: L[tuple],
        obj_pk_name: str,
    ) -> StringIO:
        """
        Function takes in a path to a delimited file and returns a IO object
        where the identifying columns have been hashed into a primary key in the
        first ordinal position of the table. The hash uses the id_column_names
        so that only ID info is hashed into the hash value
        """
        # All ro
        output_file_obj = StringIO()
        for i, (pk_curr, row_curr) in enumerate(set(zip(pk, data))):
            new_line = [str(pk_curr)] + list(row_curr)  # type: ignore
            new_line = map(str, new_line)  # type: ignore
            new_line = map(  # type: ignore
                lambda x: x.replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\\", "\\\\"),
                new_line,
            )
            output_file_obj.write("\t".join(new_line) + "\n")  # type: ignore

        output_file_obj.seek(0)

        return output_file_obj

    def _load(
        self,
        cxn: Conn,
        universe: "UNIVERSE_TYPE",
        rows: L[dict],
        insert: bool,
        test: bool = False,
    ) -> L[int]:
        """
        Helpful docstring
        """
        self._logger.debug("recursively loading foreign keys")
        for kk, vv in self.fks.items():
            if vv.insert:
                vv._load(cxn, universe, rows, insert=True, test=test)

        self._logger.debug("Getting attributes and generating hashes")
        obj_pk_name = universe[self.obj][0]
        pk, data = [], []
        for row in rows:
            pk_curr, data_curr = self._getvals(universe, row)
            pk.extend(pk_curr)
            data.extend(data_curr)

        self._logger.debug("writing data to stringio object")
        io_obj = self._data_to_stringIO(pk, data, obj_pk_name)
        if not data:
            return []

        # If we are testing don't try and do sql
        if not test:
            self._load_data(cxn, obj_pk_name, io_obj, insert)

        return [int(x) for x in pk]

    def _load_data(self, cxn: Conn, obj_pk_name: str, io_obj: StringIO, insert: bool) -> None:
        """
        Function that quickly loads an io_obj import the database specified
        obj_pk_name. Insert flag determines whether we update or insert.

        Args:
            cxn (Conn): connection to database to load intop
            obj_pk_name (str): name of objects primary key name
            io_obj (StringIO): StringIO to use for copy_from into database
            insert (bool): whether or not to update or insert into database

        Raises:
            ValueError: [description]
            DBgenExternalError: [description]
            ValueError: [description]
        """
        # Temporary table to copy data into
        # Set name to be hash of input rows to ensure uniqueness for parallelization
        temp_table_name = self.obj + "_temp_load_table_" + str(getrandbits(64))

        # Need to create a temp table to copy data into
        # Add an auto_inc column so that data can be ordered by its insert location

        create_temp_table = """
        DROP TABLE IF EXISTS {temp_table_name};
        CREATE TEMPORARY TABLE {temp_table_name} AS
        TABLE {obj}
        WITH NO DATA;
        ALTER TABLE {temp_table_name}
        ADD COLUMN auto_inc SERIAL NOT NULL;
        """.format(
            obj=self.obj, temp_table_name=temp_table_name
        )

        cols = [obj_pk_name] + list(sorted(self.attrs.keys())) + list(sorted(self.fks.keys()))
        from dbgen.templates import jinja_env

        if insert:
            template = jinja_env.get_template("insert.sql.jinja")
        else:
            template = jinja_env.get_template("update.sql.jinja")

        first = False
        update = True
        fk_cols = self.fks.keys()
        template_args = dict(
            obj=self.obj,
            obj_pk_name=obj_pk_name,
            partition_attr=self.partition_attr,
            temp_table_name=temp_table_name,
            all_column_names=cols,
            fk_cols=fk_cols,
            first=first,
            update=update,
        )
        load_statement = template.render(**template_args)

        with cxn.cursor() as curs:
            # Create the temp table
            curs.execute(create_temp_table)
            # Attempt the loading step 3 times
            self._logger.debug("load into temporary table")
            query_fail_count = 0
            while True:
                if query_fail_count == NUM_QUERY_TRIES:
                    raise ValueError("Query Cancel fail max reached")
                try:
                    curs.copy_from(
                        io_obj,
                        temp_table_name,
                        null="None",
                        columns=cols,
                    )
                    break
                except QueryCanceled:
                    print("Query cancel failed")
                    query_fail_count += 1
                    continue
                except Psycopg2Error as exc:
                    raise DBgenExternalError(exc.pgerror)

            # Try to insert everything from the temp table into the real table
            # If a foreign_key violation is hit, we delete those rows in the
            # temp table and move on
            fk_fail_count = 0
            self._logger.debug("transfer from temp table to main table")
            while True:
                if fk_fail_count == 10:
                    raise ValueError("User Canceled due to large number of FK violations")
                # check for ForeignKeyViolation error
                try:
                    curs.execute(load_statement)
                    break
                except psycopg2.errors.ForeignKeyViolation as exc:
                    pattern = r'Key \((\w+)\)=\(([\-\d]+)\) is not present in table "(\w+)"'
                    fk_name, fk_pk, fk_obj = re.findall(pattern, exc.pgerror)[0]
                    delete_statement = f"delete from {temp_table_name} where {fk_name} = {fk_pk}"
                    curs.execute(delete_statement)
                    self._logger.error(
                        "---\n"
                        f"ForeignKeyViolation #({fk_fail_count+1}): tried to insert {fk_pk} into"
                        f" FK column {fk_name} of {self.obj}."
                        f"\nBut no row exists with {fk_obj}_id = {fk_pk} in {fk_obj}."
                    )
                    self._logger.error(f"Moving on without inserting any rows with this {fk_pk}")
                    self._logger.error(exc)
                    fk_fail_count += 1
                    continue
            if fk_fail_count:
                self._logger.error(f"Fail count = {fk_fail_count}")

        io_obj.close()
        self._logger.debug("loading finished")

    def test(self, universe: "UNIVERSE_TYPE", rows: L[dict]) -> D[str, L[dict]]:
        """
        Takes in the universe and processed namespaces and generates dict where keys are table names and values are lists of input rows

        Args:
            universe (D[str, T[str, L[str], L[str]]]): universe of model
            rows (L[dict]): example processed namespaces after PyBlocks applied

        Returns:
            D[str, L[dict]]: dictionary of mapping tables to lists of rows that would be inserted if this load were called
        """
        obj_pk_name, ids, id_fks, dtype_dict = universe[self.obj]
        pk, data = [], []

        for row in rows:
            pk_curr, data_curr = self._getvals(universe, row)
            pk.extend(pk_curr)
            data.extend(data_curr)

            for kk, vv in sorted(self.fks.items()):
                if vv.pk is not None:
                    val = vv.pk.arg_get(row)
                else:
                    val, fk_adata = vv._getvals(universe, row)

        io_obj = self._data_to_stringIO(pk, data, obj_pk_name)

        cols = [obj_pk_name] + list(sorted(self.attrs.keys())) + list(sorted(self.fks.keys()))
        table_rows = []
        while True:
            line = io_obj.readline()
            if not line:
                break
            table_rows.append({col: val for col, val in zip(cols, line.strip("\n").split("\t"))})

        output = {self.obj + ("_insert" if self.insert else ""): table_rows}
        # Save the rows of recursive loads
        for kk, vv in self.fks.items():
            if vv.insert:
                if vv.obj == self.obj and vv.insert == self.insert:
                    print("!WARNING! self FKs aren't viewable in interact mode")
                else:
                    output.update(vv.test(universe, rows))
        return output

    def make_src(self) -> str:
        """
        Output a stringified version of load that can be run in an Airflow PythonOperator
        """
        attrs = ",".join([f"{k}={v.make_src(meta=True)}" for k, v in self.attrs.items()])
        template = (
            "Load(obj= '{{ obj }}',attrs= dict({{attrs}}),"
            "fks=dict({{ fks }}),pk= {{ pk }},insert={{ insert }})"
        )
        fks = ",".join([f"{k}={v.make_src()}" for k, v in self.fks.items()])
        pk = None if self.pk is None else self.pk.make_src(meta=True)
        return Template(template).render(obj=self.obj, attrs=attrs, fks=fks, pk=pk, insert=self.insert)
