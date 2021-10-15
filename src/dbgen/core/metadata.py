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

from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic.types import Json
from sqlalchemy import Column, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import registry
from sqlalchemy.sql.expression import text
from sqlmodel import Field, select
from sqlmodel.sql.sqltypes import GUID, AutoString

from dbgen.configuration import config
from dbgen.core.entity import Entity, id_field
from dbgen.utils.sql import create_view

META_SCHEMA = config.meta_schema
meta_registry = registry()


class Status(str, Enum):
    initialized = "initialized"
    excluded = "excluded"
    running = "running"
    failed = "failed"
    completed = "completed"
    testing = "testing"


class Root(Entity):
    __schema__ = META_SCHEMA


class RunEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "run"
    id: Optional[int] = Field(None, sa_column_kwargs={"autoincrement": True, "primary_key": True})
    status: Optional[Status]
    nuke: Optional[bool]
    only: Optional[str]
    exclude: Optional[str]
    start: Optional[str]
    until: Optional[str]
    runtime: Optional[float]
    errors: int = 0


class GeneratorEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "generator"
    id: Optional[UUID] = id_field
    name: str
    query: Optional[str]
    description: Optional[str]
    batch_size: Optional[int]
    tables_needed: Optional[str]
    table_yielded: Optional[str]
    column_needed: Optional[str]
    column_yielded: Optional[str]
    gen_json: Optional[Json] = Field(None, sa_column=Column(JSONB))


class GeneratorRunEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "generator_run"
    generator_id: Optional[UUID] = GeneratorEntity.foreign_key(primary_key=True)
    run_id: Optional[int] = RunEntity.foreign_key(primary_key=True)
    ordering: Optional[int]
    status: Optional[Status]
    runtime: Optional[float]
    number_of_extracted_rows: Optional[int] = 0
    number_of_inputs_processed: int = 0
    unique_inputs: int = 0
    skipped_inputs: int = 0
    error: Optional[str]


class Repeats(Root, registry=meta_registry, table=True):
    __tablename__ = "repeats"
    generator_id: Optional[UUID] = GeneratorEntity.foreign_key()
    input_hash: Optional[UUID] = Field(None, primary_key=True)


run_view_statement = (
    select(
        GeneratorEntity.name,
        *(
            getattr(GeneratorRunEntity, x)
            for x in GeneratorRunEntity.__fields__
            if x not in ("generator_id",)
        ),
    )
    .join_from(GeneratorRunEntity, GeneratorEntity)
    .order_by(GeneratorRunEntity.run_id.desc(), GeneratorRunEntity.ordering)  # type: ignore
)

current_run_view_statement = run_view_statement.where(
    GeneratorRunEntity.run_id == select(func.max(RunEntity.id)).scalar_subquery()  # type: ignore
)
failed_run_view_statement = run_view_statement.where(
    GeneratorRunEntity.status.in_(("failed", "running"))  # type: ignore
)


class CurrentRun(Entity, registry=meta_registry):
    __table__ = create_view(
        "current_run",
        current_run_view_statement.subquery(),
        META_SCHEMA,
        meta_registry.metadata,
    )


class FailedRuns(Entity, registry=meta_registry):
    __table__ = create_view(
        "failed_run",
        failed_run_view_statement.subquery(),
        META_SCHEMA,
        meta_registry.metadata,
    )


gens_to_run_stmt = text(
    f"""
select
    distinct
    gen.name, cr.generator_id, coalesce(gr_bad.status, 'never run') as last_status, gr_bad.error
from
    {META_SCHEMA}.generator_run cr
join {META_SCHEMA}.generator gen on
    gen.id = cr.generator_id
left join {META_SCHEMA}.generator_run gr_completed on
    cr.generator_id = gr_completed.generator_id
    and gr_completed.status = 'completed'
left join {META_SCHEMA}.generator_run gr_bad on
    cr.generator_id = gr_bad.generator_id
    and gr_bad.status = 'failed'
where
    cr.status != 'completed'
    and cr.run_id = (
    select
        max(id)
    from
        {META_SCHEMA}.run)
    and (gr_completed.run_id is null
        or
     gr_bad.run_id>gr_completed.run_id)
"""
)


class GensToRun(Entity, registry=meta_registry):
    __table__ = create_view(
        "gens_to_run",
        gens_to_run_stmt,
        META_SCHEMA,
        meta_registry.metadata,
        [
            Column("name", AutoString()),
            Column("generator_id", GUID()),
            Column("last_status", AutoString()),
            Column("error", AutoString()),
        ],
    )