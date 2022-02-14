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
from functools import partial
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from uuid import UUID

import psycopg2
from psycopg import Connection as PG3Conn
from psycopg2._psycopg import connection
from pydantic import Field, PrivateAttr
from pydantic import ValidationError as PydValidationError
from pydantic import root_validator, validate_model, validator
from pydantic.error_wrappers import ErrorWrapper
from pydasher import hasher
from pydasher.import_module import import_string

from dbgen.configuration import ValidationEnum, config
from dbgen.core.args import Arg, Constant
from dbgen.core.base import Base
from dbgen.core.dependency import Dependency
from dbgen.core.node.computational_node import ComputationalNode
from dbgen.core.type_registry import column_registry
from dbgen.exceptions import ValidationError
from dbgen.utils.lists import broadcast, is_broadcastable

if TYPE_CHECKING:
    from dbgen.core.entity import BaseEntity  # pragma: no cover


def hash_tuple(tuple_to_hash: Tuple[Any, ...]) -> UUID:
    return UUID(hasher(tuple_to_hash))


class LoadEntity(Base):
    """Object for passing minimum info to the Load object for database insertion."""

    name: str
    schema_: Optional[str]
    entity_class_str: Optional[str]
    primary_key_name: str
    identifying_attributes: Set[str] = Field(default_factory=dict)
    identifying_foreign_keys: Set[str] = Field(default_factory=set)
    attributes: Dict[str, str] = Field(default_factory=dict)
    required: Set[str] = Field(default_factory=set)
    foreign_keys: Set[str] = Field(default_factory=set)
    _entity: Optional[Type['BaseEntity']] = PrivateAttr(None)

    def __str__(self):
        return (
            "LoadEntity<"
            f"name={self.name!r}, "
            f"schema={self.schema!r}, "
            f"id_attrs={self.identifying_attributes}, "
            f"id_fks={self.identifying_foreign_keys}, "
            f"attributes={self.attributes}, "
            f"required={self.required}, "
            f"fks={self.foreign_keys}>"
        )

    def _load_entity(self) -> Optional[Type['BaseEntity']]:
        if self._entity is None:
            if self.entity_class_str is None:
                self._logger.warning(f"No Entity Class String found cannot validate data!")
                return None
            try:
                entity = import_string(self.entity_class_str)
                self._entity: 'BaseEntity' = entity
            except ImportError:
                self._logger.warning(
                    f"Cannot load entity from class string {self.entity_class_str} cannot validate data!"
                )
                return None
        return self._entity

    # TODO Merge validate and strict validate into one method
    # TODO Decide whether use default is a valid flag for dbgen
    def _validate(self, input_data: Dict[str, Any], use_defaults: bool = False, insert: bool = False):
        entity = self._load_entity()
        if entity is None:
            return input_data
        validated_data, fields_set, errors = validate_model(entity, input_data)
        # If we have errors during validation check if we are inserting
        # if we are not inserting we ignore missing values as updates don't require all info
        # TODO decide whether there is a better way to handle required info
        if errors:
            sub_errors = errors.errors()
            # If insert is not true we cannot check required fields as we are inserting with primary key
            if not insert:
                sub_errors = list(filter(lambda x: x.get('type') != 'value_error.missing', sub_errors))
            # if we still have sub-errors raise the errors to the user
            if sub_errors:
                raise errors
        base_fields = {'id', 'etl_step_id', 'created_at'}
        return {
            k: v
            for k, v in validated_data.items()
            if k not in base_fields and (use_defaults or k in fields_set)
        }

    def _strict_validate(self, input_data: Dict[str, Any], use_defaults: bool = False, insert: bool = False):
        # Check basic validation
        self._validate(input_data, use_defaults=use_defaults, insert=insert)
        # Add additional check for expected type for strict typing
        errors = []
        for key, val in input_data.items():
            type_str = self.attributes[key]
            # if the type str ends with brackets the expected type is an array
            # TODO check the elements types for lists
            if type_str.endswith('[]'):
                expected_type: type = list
            else:
                expected_type = column_registry[type_str].get_python_type()
            # If the val is not the expected type append a TypeError and continue on so we can collect all the errors
            if not isinstance(val, expected_type):
                errors.append(
                    ErrorWrapper(
                        TypeError(
                            "Strict validation found an error:\n"
                            f"{key!r} on Enitity {self.name!r} has column type of {type_str!r} which expects a python type of {expected_type!r}, but provided value was type {type(val)}"
                        ),
                        loc=key,
                    )
                )
        # If we found any errors raise them
        if errors:
            raise PydValidationError(errors, self.__class__)
        return input_data

    def _get_hash(self, arg_dict: Dict[str, Any]) -> UUID:
        id_fks = (str(arg_dict[val]) for val in sorted(self.identifying_foreign_keys))
        id_attrs = []
        for attr_name in sorted(self.identifying_attributes):
            type_str = self.attributes[attr_name]
            data_type = column_registry[type_str]
            type_func = list if type_str == data_type.array_oid else data_type.python_type
            try:
                arg_val = arg_dict[attr_name]
                if arg_val is None or isinstance(arg_val, type_func):
                    coerced_val = arg_val
                else:
                    raise TypeError(
                        f"Type Coercing is turned off. You are trying to insert into attribute {self.name}({attr_name}) which has a type of {type_func} but you provided a type {type(arg_val)}.\n"
                        "If you want to turn Type Coercement on set the configuration variable DBGEN_TYPE_COERCING=true in your config file or environment variable"
                    )
            except TypeError as exc:
                raise ValueError(f"Error coercing value {arg_val!r} to type {type_func}:\n{exc}") from exc
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


NUM_QUERY_TRIES = 10

T = TypeVar('T')


class Load(ComputationalNode[T]):
    load_entity: LoadEntity
    primary_key: Optional[Union[Arg, Constant]] = None
    _output: Dict[UUID, Iterable[Any]] = PrivateAttr(default_factory=dict)
    insert: bool = False
    validation: Optional[ValidationEnum] = None
    outputs: List[str] = Field(default_factory=list)
    # _logger_name: ClassVar[
    #     Callable[["Base", Dict[str, Any]], str]
    # ] = lambda _, kwargs: f"dbgen.load.{kwargs.get('load_entity').name}"  # type: ignore

    @root_validator
    def change_output_name(cls, values):
        load_entity = values.get('load_entity')
        values['outputs'] = [f"{load_entity.name}_id"]
        return values

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
        if isinstance(primary_key, Constant):
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
        pk_is_missing = pk is None or (isinstance(pk, Constant) and pk.val is None)
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

    def new_run(
        self, row: Dict[str, Mapping[str, Any]], rows_to_load: Dict[str, Dict[UUID, Any]]
    ) -> Dict[str, List[UUID]]:
        not_list = lambda x: not isinstance(x, (list, tuple))
        lists_allowed = lambda x: self.load_entity.attributes[x].endswith('[]')
        arg_dict = {
            key: [val] if not_list(val) or lists_allowed(key) else val
            for key, val in sorted(self._get_inputs(row).items())
        }
        # Check for empty lists, as that will cause the row to be ignored
        if any(map(lambda x: len(x) == 0, arg_dict.values())):
            # self._logger.debug(f'Row {arg_dict} produced 0 rows for load {self}')
            return {self.outputs[0]: []}
        # Check for broadcastability
        try:
            is_broadcastable(*arg_dict.values())
        except ValueError as exc:
            raise ValueError(
                f"While assembling rows for loading into {self} found two sequences in the inputs to the load with non-equal, >1 length\n"
                "This can occur when two outputs from pyblocks/queries are unequal in length.\n"
                "If so, please make the relevant cartesian product of the two lists before loading\n"
            ) from exc
        try:
            broadcasted_values = [
                {key: val for key, val in zip(arg_dict.keys(), row)} for row in broadcast(*arg_dict.values())
            ]
        except ValueError as exc:
            raise ValueError(
                f"Error occurred during the loading of row {(str(arg_dict)[:100])} into entity {self.load_entity.full_name}"
            ) from exc

        # Load
        validation = self.validation or config.validation
        if validation == ValidationEnum.STRICT:
            validation_func = self.load_entity._strict_validate
        elif validation == ValidationEnum.COERCE:
            validation_func = self.load_entity._validate

        try:
            broadcasted_values = list(map(partial(validation_func, insert=self.insert), broadcasted_values))
        except PydValidationError as exc:
            raise ValidationError(
                f"Error occurred during data validation while loading into {self.load_entity.full_name!r}:\n{exc}"
            ) from exc
        # If we have a user supplied Primary Key go get it and broadcast it
        if self.primary_key is not None:
            primary_arg_val = self.primary_key.arg_get(row)
            # Validate the primary key type
            if isinstance(primary_arg_val, UUID):
                primary_keys: List[UUID] = [primary_arg_val]
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
        sorted_keys = sorted(self.inputs.keys())
        # Update rows to load dict in place
        rows_to_load[self.hash].update(
            {
                primary_key: [value[key] for key in sorted_keys]
                for primary_key, value in zip(primary_keys, broadcasted_values)
            }
        )

        return {self.outputs[0]: primary_keys}

    def run(self, row: Dict[str, Mapping[str, Any]]) -> Dict[str, List[UUID]]:
        not_list = lambda x: not isinstance(x, (list, tuple))
        lists_allowed = lambda x: self.load_entity.attributes[x].endswith('[]')
        arg_dict = {
            key: [val] if not_list(val) or lists_allowed(key) else val
            for key, val in sorted(self._get_inputs(row).items())
        }
        # Check for empty lists, as that will cause the row to be ignored
        if any(map(lambda x: len(x) == 0, arg_dict.values())):
            self._logger.debug(f'Row {arg_dict} produced 0 rows for load {self}')
            return {self.outputs[0]: []}
        # Check for broadcastability
        try:
            is_broadcastable(*arg_dict.values())
        except ValueError as exc:
            raise ValueError(
                f"While assembling rows for loading into {self} found two sequences in the inputs to the load with non-equal, >1 length\n"
                "This can occur when two outputs from pyblocks/queries are unequal in length.\n"
                "If so, please make the relevant cartesian product of the two lists before loading\n"
            ) from exc
        try:
            broadcasted_values = [
                {key: val for key, val in zip(arg_dict.keys(), row)} for row in broadcast(*arg_dict.values())
            ]
        except ValueError as exc:
            raise ValueError(
                f"Error occurred during the loading of row {(str(arg_dict)[:100])} into entity {self.load_entity.full_name}"
            ) from exc

        # Load
        validation = self.validation or config.validation
        if validation == ValidationEnum.STRICT:
            validation_func = self.load_entity._strict_validate
        elif validation == ValidationEnum.COERCE:
            validation_func = self.load_entity._validate

        try:
            broadcasted_values = list(map(partial(validation_func, insert=self.insert), broadcasted_values))
        except PydValidationError as exc:
            raise ValidationError(
                f"Error occurred during data validation while loading into {self.load_entity.full_name!r}:\n{exc}"
            ) from exc
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
        sorted_keys = sorted(self.inputs.keys())
        self._output.update(
            {
                primary_key: [value[key] for key in sorted_keys]
                for primary_key, value in zip(primary_keys, broadcasted_values)
            }
        )
        return {self.outputs[0]: primary_keys}

    def load(self, cxn: connection, etl_step_id: UUID):
        """Run the Load statement for the given namespace rows.

        Args:
            namespace_rows (List[Dict[str, Any]]): A dictionary with the strings as hashes and the values as the local namespace dictionaries from PyBlocks, Queries, and Consts
        """
        self._logger.debug(f"Loading into {self.load_entity.name}")
        self._load_data(data=self._output, connection=cxn, etl_step_id=etl_step_id)
        self._output = {}

    def _get_types(self):
        oids = list(map(lambda x: self.load_entity.attributes[x], sorted(self.inputs.keys())))
        return oids

    def _load_data(self, data, connection: PG3Conn, etl_step_id: UUID) -> None:
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
        ALTER COLUMN "etl_step_id" SET DEFAULT '{etl_step_id}';
        """.format(
            obj=self.load_entity.name,
            schema=self.load_entity.schema_,
            temp_table_name=temp_table_name,
            etl_step_id=etl_step_id,
        )
        with connection.cursor() as cur:
            cur.execute(drop_temp_table)
            cur.execute(create_temp_table)
        connection.commit()

        cols = [self.load_entity.primary_key_name] + list(sorted(self.inputs.keys()))
        col_str = ','.join(map(lambda x: f"\"{x}\"", cols))
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
            all_column_names=cols + ["etl_step_id"],
            first=first,
            update=update,
            schema=self.load_entity.schema_,
        )
        load_statement = template.render(**template_args)
        with connection.cursor() as cur:
            self._logger.debug("load into temporary table")
            with cur.copy(f'COPY  {temp_table_name} ({col_str}) FROM STDIN') as copy:
                oids = self._get_types()
                copy.set_types(oids)
                for pk_curr, row_curr in data.items():
                    copy.write_row((pk_curr, *row_curr))

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
                    cur.execute(load_statement)
                    break
                except (psycopg2.errors.SyntaxError, psycopg2.errors.UndefinedColumn):
                    print(load_statement)
                    raise
                except psycopg2.errors.ForeignKeyViolation as exc:
                    pattern = r'Key \((\w+)\)=\(([\-\d]+)\) is not present in table "(\w+)"'
                    fk_name, fk_pk, fk_obj = re.findall(pattern, exc.pgerror)[0]
                    delete_statement = f"delete from {temp_table_name} where {fk_name} = {fk_pk}"
                    cur.execute(delete_statement)
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
            cur.execute(drop_temp_table)
        connection.commit()
        self._logger.debug("loading finished")
