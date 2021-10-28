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

import re
from io import StringIO
from itertools import chain
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple, Union
from uuid import UUID

import psycopg2
from psycopg2._psycopg import connection
from psycopg2.errors import Error as Psycopg2Error
from psycopg2.errors import QueryCanceled
from pydantic import Field, root_validator, validator
from pydantic.fields import PrivateAttr
from pydasher import hasher

from dbgen.configuration import config
from dbgen.core.args import Arg, Const
from dbgen.core.base import Base
from dbgen.core.dependency import Dependency
from dbgen.core.node.computational_node import ComputationalNode
from dbgen.exceptions import DBgenExternalError
from dbgen.utils.lists import broadcast, is_broadcastable
from dbgen.utils.type_coercion import SQLTypeEnum, get_python_type, get_str_converter


def hash_tuple(tuple_to_hash: Tuple[Any, ...]) -> UUID:
    return UUID(hasher(tuple_to_hash))


class LoadEntity(Base):
    """Object for passing minimum info to the Load object for database insertion."""

    name: str
    schema_: Optional[str]
    primary_key_name: str
    identifying_attributes: Set[str] = Field(default_factory=dict)
    identifying_foreign_keys: Set[str] = Field(default_factory=set)
    attributes: Dict[str, SQLTypeEnum] = Field(default_factory=dict)
    foreign_keys: Set[str] = Field(default_factory=set)

    def __str__(self):
        return (
            "LoadEntity<"
            f"name={self.name!r}, "
            f"schema={self.schema!r}, "
            f"id_attrs={self.identifying_attributes}, "
            f"id_fks={self.identifying_foreign_keys}, "
            f"attributes={self.attributes}, "
            f"fks={self.foreign_keys}>"
        )

    def _get_hash(self, arg_dict: Dict[str, Any]) -> UUID:
        id_fks = (str(arg_dict[val]) for val in sorted(self.identifying_foreign_keys))
        id_attrs = []
        for attr_name in sorted(self.identifying_attributes):
            type_ = self.attributes[attr_name]
            type_func = get_python_type(type_)
            try:
                arg_val = arg_dict[attr_name]
                if arg_val is None or isinstance(arg_val, type_func):
                    coerced_val = arg_val
                else:
                    if not config.type_coercing:
                        raise TypeError(
                            f"Type Coercing is turned off. You are trying to insert into attribute {self.name}({attr_name}) which has a type of {type_func} but you provided a type {type(arg_val)}.\n"
                            "If you want to turn Type Coercement on set the configuration variable DBGEN_TYPE_COERCING=true in your config file or environment variable"
                        )
                    coerced_val = type_func(arg_val)
            except TypeError as exc:
                raise ValueError(f"Error coercing value {arg_val!r} to type {type_}:\n{exc}") from exc
            except KeyError:
                raise KeyError(f"Cannot find id_attribute {attr_name!r} in arg_dict for hashing: {arg_dict}")
            id_attrs.append(coerced_val)
        tuple_to_hash = (*id_attrs, *id_fks)
        return hash_tuple(tuple_to_hash)

    @property
    def full_name(self) -> str:
        return f"{self.schema_}.{self.name}" if self.schema_ else self.name

    @property
    def identifiers(self) -> Set[str]:
        return self.identifying_foreign_keys.union(self.identifying_attributes)

    def _get_str_converter(self, key) -> Callable[[Any], str]:
        if key in self.attributes:
            return get_str_converter(self.attributes[key])
        elif key in self.foreign_keys:
            return get_str_converter(SQLTypeEnum.UUID)
        raise ValueError(f"Unknown str converter for key {key}")


NUM_QUERY_TRIES = 10


class Load(ComputationalNode):
    load_entity: LoadEntity
    primary_key: Optional[Union[Arg, Const]] = None
    _output: Dict[UUID, Tuple[Any, ...]] = PrivateAttr(default_factory=dict)
    insert: bool = False

    # _logger_name: ClassVar[
    #     Callable[["Base", Dict[str, Any]], str]
    # ] = lambda _, kwargs: f"dbgen.load.{kwargs.get('load_entity').name}"  # type: ignore

    @root_validator
    def check_bad_insert(cls, values):
        primary_key = values.get("primary_key")
        insert = values.get("insert")
        if primary_key is not None and insert:
            raise ValueError(
                f"Can't insert into {values.get('load_entity')} if we already have Primary Key..."
            )
        return values

    @validator("primary_key")
    def check_const_primary_key(cls, primary_key):
        if isinstance(primary_key, Const):
            assert (
                primary_key.val is None
            ), f"Currently don't allow const primary keys to be used unless None is the value: {primary_key}"
        return primary_key

    @root_validator
    def check_required_info(cls, values):
        insert = values.get("insert")
        load_entity = values.get("load_entity")
        inputs = values.get("inputs")
        pk = values.get("primary_key")
        if load_entity is None:
            raise ValueError("Missing load_entity")
        pk_is_missing = pk is None or (isinstance(pk, Const) and pk.val is None)
        if insert or pk_is_missing:
            missing_attrs = filter(lambda x: x not in inputs, load_entity.identifying_attributes)
            missing_fks = filter(lambda x: x not in inputs, load_entity.identifying_foreign_keys)
            missing = list(chain(missing_attrs, missing_fks))
            if missing:
                action = "insert" if insert else "update"
                raise ValueError(f"Can't {action} {load_entity} row without required ID info {missing}: {pk}")
        return values

    def __str__(self):
        return f"Load<{self.load_entity.name}, {len(self.inputs)} attrs/fks>"

    def _get_dependency(self) -> Dependency:
        tables_yielded: Set[str] = set()
        tables_needed: Set[str] = set()
        if self.insert:
            tables_yielded.add(self.load_entity.full_name)
        else:
            tables_needed.add(self.load_entity.full_name)

        columns_yielded: Set[str] = set()
        for attr in self.inputs:
            if self.insert or (attr not in self.load_entity.identifiers):
                columns_yielded.add(f"{self.load_entity.full_name}.{attr}")

        if not self.insert:
            columns_needed = {
                f"{self.load_entity.full_name}.{attr}"
                for attr in self.inputs
                if attr in self.load_entity.identifiers
            }
        else:
            columns_needed = set()

        return Dependency(
            tables_yielded=tables_yielded,
            columns_yielded=columns_yielded,
            columns_needed=columns_needed,
            tables_needed=tables_needed,
        )

    def run(self, row: Dict[str, Mapping[str, Any]]) -> Dict[str, List[UUID]]:
        not_list = lambda x: not isinstance(x, (list, tuple))
        arg_dict = {
            key: [val] if not_list(val) else val for key, val in sorted(self._get_inputs(row).items())
        }
        # Check for broadcastability
        try:
            is_broadcastable(*arg_dict.values())
        except ValueError as exc:
            raise ValueError(
                "While assembling rows for loading found two sequences in the inputs to the load with non-equal, >1 length\n"
                "This can occur when two outputs from pyblocks/queries are unequal in length.\n"
                "If so, please make the relevant cartesian product of the two lists before loading\n"
            ) from exc
        broadcasted_values = [
            {key: val for key, val in zip(arg_dict.keys(), row)} for row in broadcast(*arg_dict.values())
        ]
        # If we have a user supplied Primary Key go get it and broadcast it
        if self.primary_key is not None:
            primary_arg_val = self.primary_key.arg_get(row)
            # Validate the primary key type
            if isinstance(primary_arg_val, UUID):
                primary_keys = [primary_arg_val]
            elif isinstance(primary_arg_val, Sequence):
                assert all(map(lambda x: isinstance(x, UUID), primary_arg_val))
                primary_keys = list(primary_arg_val)
            else:
                ValueError(f"Unknown Primary Key Type: {primary_arg_val}")

            if len(primary_keys) == 1:
                primary_keys *= len(broadcasted_values)
            elif len(primary_keys) != len(broadcasted_values):
                raise ValueError(
                    f"Cannot broadcast Primary Key to Max Length: {len(primary_keys)} {len(broadcasted_values)}"
                )
        else:
            # If we don't have a primary key get it from the identifying info on the broadcasted
            # Values
            primary_keys = [self.load_entity._get_hash(row) for row in broadcasted_values]
        self._output.update(
            {
                primary_key: tuple(value.values())
                for primary_key, value in zip(primary_keys, broadcasted_values)
            }
        )
        return {"out": primary_keys}

    def load(self, cxn: connection, gen_id: UUID):
        """Run the Load statement for the given namespace rows.

        Args:
            namespace_rows (List[Dict[str, Any]]): A dictionary with the strings as hashes and the values as the local namespace dictionaries from PyBlocks, Queries, and Consts
        """
        self._logger.debug(f"Loading into {self.load_entity.name}")
        io_obj = self._data_to_stringIO()
        self._load_data(io_obj=io_obj, cxn=cxn, gen_id=gen_id)
        self._output = {}

    def _data_to_stringIO(
        self,
    ) -> StringIO:
        """
        Function takes in a path to a delimited file and returns a IO object
        where the identifying columns have been hashed into a primary key in the
        first ordinal position of the table. The hash uses the id_column_names
        so that only ID info is hashed into the hash value
        """
        # All ro
        output_file_obj = StringIO()
        str_converters = [
            self.load_entity._get_str_converter(k) for k in chain(['id'], sorted(self.inputs.keys()))
        ]
        for pk_curr, row_curr in self._output.items():
            new_line = [str(pk_curr)] + list(row_curr)  # type: ignore
            new_line = map(lambda x, y: x(y), str_converters, new_line)  # type: ignore
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

    def _load_data(self, cxn: connection, io_obj: StringIO, gen_id: UUID) -> None:
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
        temp_table_name = self.load_entity.name + "_temp_load_table_" + self.hash

        # Need to create a temp table to copy data into
        # Add an auto_inc column so that data can be ordered by its insert location

        drop_temp_table = f"DROP TABLE IF EXISTS {temp_table_name};"
        create_temp_table = """
        CREATE TEMPORARY TABLE {temp_table_name} AS
        TABLE {schema}.{obj}
        WITH NO DATA;
        ALTER TABLE {temp_table_name}
        ADD COLUMN auto_inc SERIAL NOT NULL;
        ALTER TABLE {temp_table_name}
        ALTER COLUMN "gen_id" SET DEFAULT '{gen_id}';
        """.format(
            obj=self.load_entity.name,
            schema=self.load_entity.schema_,
            temp_table_name=temp_table_name,
            gen_id=gen_id,
        )

        cols = [self.load_entity.primary_key_name] + list(sorted(self.inputs.keys()))
        from dbgen.templates import jinja_env

        if self.insert:
            template = jinja_env.get_template("insert.sql.jinja")
        else:
            template = jinja_env.get_template("update.sql.jinja")

        first = False
        update = True
        template_args = dict(
            obj=self.load_entity.name,
            obj_pk_name=self.load_entity.primary_key_name,
            temp_table_name=temp_table_name,
            all_column_names=cols + ["gen_id"],
            first=first,
            update=update,
            schema=self.load_entity.schema_,
        )
        load_statement = template.render(**template_args)

        with cxn.cursor() as curs:
            # Drop Temp Table
            curs.execute(drop_temp_table)
        with cxn.cursor() as curs:
            # Create the temp tabletable)
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
                except QueryCanceled:  # type: ignore
                    print("Query cancel failed")
                    query_fail_count += 1
                    continue
                except Psycopg2Error as exc:  # type: ignore
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
                except (psycopg2.errors.SyntaxError, psycopg2.errors.UndefinedColumn):
                    print(load_statement)
                    raise
                except psycopg2.errors.ForeignKeyViolation as exc:
                    pattern = r'Key \((\w+)\)=\(([\-\d]+)\) is not present in table "(\w+)"'
                    fk_name, fk_pk, fk_obj = re.findall(pattern, exc.pgerror)[0]
                    delete_statement = f"delete from {temp_table_name} where {fk_name} = {fk_pk}"
                    curs.execute(delete_statement)
                    self._logger.error(
                        "---\n"
                        f"ForeignKeyViolation #({fk_fail_count+1}): tried to insert {fk_pk} into"
                        f" FK column {fk_name} of {self.load_entity.name}."
                        f"\nBut no row exists with {fk_obj}_id = {fk_pk} in {fk_obj}."
                    )
                    self._logger.error(f"Moving on without inserting any rows with this {fk_pk}")
                    self._logger.error(exc)
                    fk_fail_count += 1
                    continue
            if fk_fail_count:
                self._logger.error(f"Fail count = {fk_fail_count}")
            self._logger.debug("Dropping temp table...")
            curs.execute(drop_temp_table)
        io_obj.close()
        cxn.commit()
        self._logger.debug("loading finished")
