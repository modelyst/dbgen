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
    upstream_failed = "upstream_failed"


class Root(BaseEntity):
    __schema__ = META_SCHEMA


class ModelETLStepMap(Root, registry=meta_registry, table=True):
    __tablename__ = 'model_etl_step'
    model_id: Optional[UUID] = Field(None, foreign_key=f"{META_SCHEMA}.model.id", primary_key=True)
    etl_step_id: Optional[UUID] = Field(None, foreign_key=f"{META_SCHEMA}.etl_step.id", primary_key=True)


class ModelEntity(Root, registry=meta_registry, table=True):
    __tablename__ = 'model'
    id: Optional[UUID] = id_field
    created_at: Optional[datetime] = get_created_at_field()
    last_run: Optional[datetime]
    name: str
    graph_json: Optional[dict]
    tags: List[str] = Field(default_factory=list)
    etl_steps: List['ETLStepEntity'] = Relationship(back_populates='models', link_model=ModelETLStepMap)


class ETLStepEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "etl_step"
    id: Optional[UUID] = id_field
    name: str
    query: Optional[str]
    description: Optional[str]
    batch_size: Optional[int]
    tables_needed: Optional[str]
    table_yielded: Optional[str]
    column_needed: Optional[str]
    column_yielded: Optional[str]
    etl_step_json: Optional[dict]
    etl_step_runs: List['ETLStepRunEntity'] = Relationship(back_populates='etl_step')
    models: List['ModelEntity'] = Relationship(back_populates='etl_steps', link_model=ModelETLStepMap)


class RunEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "run"
    id: Optional[int] = Field(None, sa_column_kwargs={"autoincrement": True, "primary_key": True})
    status: Optional[Status]
    nuke: Optional[bool]
    only: Optional[str]
    exclude: Optional[str]
    start: Optional[str]
    until: Optional[str]
    runtime: Optional[timedelta]
    errors: int = 0
    etl_step_runs: List['ETLStepRunEntity'] = Relationship(back_populates='run')


class ETLStepRunEntity(Root, registry=meta_registry, table=True):
    __tablename__ = "etl_step_run"
    etl_step_id: Optional[UUID] = ETLStepEntity.foreign_key(primary_key=True)
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
    run: RunEntity = Relationship(back_populates='etl_step_runs')
    etl_step: ETLStepEntity = Relationship(back_populates='etl_step_runs')


class Repeats(Root, registry=meta_registry, table=True):
    __tablename__ = "repeats"
    etl_step_id: Optional[UUID] = ETLStepEntity.foreign_key()
    input_hash: UUID = Field(..., primary_key=True)


run_view_statement = (
    select(
        ETLStepEntity.name,
        *(getattr(ETLStepRunEntity, x) for x in ETLStepRunEntity.__fields__ if x not in ("etl_step_id",)),
    )
    .join_from(ETLStepRunEntity, ETLStepEntity)
    .order_by(ETLStepRunEntity.run_id.desc(), ETLStepRunEntity.ordering)  # type: ignore
)

current_run_view_statement = run_view_statement.where(
    ETLStepRunEntity.run_id == select(func.max(RunEntity.id)).scalar_subquery()  # type: ignore
)
failed_run_view_statement = run_view_statement.where(
    ETLStepRunEntity.status.in_(("failed", "running"))  # type: ignore
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


etl_steps_to_run_stmt = text(
    f"""
select
    distinct
    etl_step.name, cr.etl_step_id, coalesce(gr_bad.status::text, 'never run') as last_status, gr_bad.error
from
    {META_SCHEMA}.etl_step_run cr
join {META_SCHEMA}.etl_step etl_step on
    etl_step.id = cr.etl_step_id
left join {META_SCHEMA}.etl_step_run gr_completed on
    cr.etl_step_id = gr_completed.etl_step_id
    and gr_completed.status = 'completed'
left join {META_SCHEMA}.etl_step_run gr_bad on
    cr.etl_step_id = gr_bad.etl_step_id
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


class ETLStepsToRun(BaseEntity, registry=meta_registry):
    __table__ = create_view(
        "etl_steps_to_run",
        etl_steps_to_run_stmt,
        META_SCHEMA,
        meta_registry.metadata,
        [
            Column("name", AutoString()),
            Column("etl_step_id", GUID()),
            Column("last_status", AutoString()),
            Column("error", AutoString()),
        ],
    )
