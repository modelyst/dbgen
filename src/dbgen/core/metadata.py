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

from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional
from uuid import UUID

import sqlalchemy.types as sa_types
from sqlalchemy import Column, func
from sqlalchemy.orm import registry
from sqlalchemy.sql.expression import text
from sqlmodel import Field, select
from sqlmodel.main import Relationship
from sqlmodel.sql.sqltypes import GUID, AutoString

from dbgen.configuration import config
from dbgen.core.entity import BaseEntity, get_created_at_field, id_field
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


class Root(BaseEntity):
    __schema__ = META_SCHEMA


class ModelGeneratorMap(Root, registry=meta_registry, table=True):
    __tablename__ = 'model_generator'
    model_id: Optional[UUID] = Field(None, foreign_key=f"{META_SCHEMA}.model.id", primary_key=True)
    generator_id: Optional[UUID] = Field(None, foreign_key=f"{META_SCHEMA}.generator.id", primary_key=True)


class ModelEntity(Root, registry=meta_registry, table=True):
    __tablename__ = 'model'
    id: Optional[UUID] = id_field
    created_at: Optional[datetime] = get_created_at_field()
    last_run: Optional[datetime]
    name: str
    graph_json: Optional[dict]
    tags: List[str] = Field(default_factory=list)
    generators: List['GeneratorEntity'] = Relationship(back_populates='models', link_model=ModelGeneratorMap)


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
    gen_json: Optional[dict]
    generator_runs: List['GeneratorRunEntity'] = Relationship(back_populates='generator')
    models: List['ModelEntity'] = Relationship(back_populates='generators', link_model=ModelGeneratorMap)


class RunEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "run"
    id: Optional[int] = Field(None, sa_column_kwargs={"autoincrement": True, "primary_key": True})
    status: Optional[Status] = Field(None, sa_column=Column('status', sa_types.Enum(Status)))
    nuke: Optional[bool]
    only: Optional[str]
    exclude: Optional[str]
    start: Optional[str]
    until: Optional[str]
    runtime: Optional[timedelta]
    errors: int = 0
    generator_runs: List['GeneratorRunEntity'] = Relationship(back_populates='run')


class GeneratorRunEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "generator_run"
    generator_id: Optional[UUID] = GeneratorEntity.foreign_key(primary_key=True)
    run_id: Optional[int] = RunEntity.foreign_key(primary_key=True)
    created_at: Optional[datetime] = get_created_at_field()
    ordering: Optional[int]
    status: Optional[str]
    runtime: Optional[float]
    unique_inputs: int = 0
    inputs_skipped: int = 0
    inputs_extracted: Optional[int] = 0
    inputs_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    query: Optional[str]
    error: Optional[str]
    run: RunEntity = Relationship(back_populates='generator_runs')
    generator: GeneratorEntity = Relationship(back_populates='generator_runs')


class Repeats(Root, registry=meta_registry, table=True):
    __tablename__ = "repeats"
    generator_id: Optional[UUID] = GeneratorEntity.foreign_key()
    input_hash: UUID = Field(..., primary_key=True)


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


class CurrentRun(BaseEntity, registry=meta_registry):
    __table__ = create_view(
        "current_run",
        current_run_view_statement.subquery(),
        META_SCHEMA,
        meta_registry.metadata,
    )


class FailedRuns(BaseEntity, registry=meta_registry):
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
    gen.name, cr.generator_id, coalesce(gr_bad.status::text, 'never run') as last_status, gr_bad.error
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


class GensToRun(BaseEntity, registry=meta_registry):
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
