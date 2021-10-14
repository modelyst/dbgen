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

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlalchemy.orm import registry
from sqlmodel import Field, Relationship

from dbgen.core.entity import Entity, EntityId

ID_TYPE = UUID

model_registry = registry()
get_fk = lambda foreign_key, primary_key=False: Field(
    default=None,
    foreign_key=f"public.{foreign_key}",
    primary_key=primary_key,
)


class SampleProcessProcessData(Entity, table=True, registry=model_registry):
    __tablename__ = "sample_process__process_data"
    process_data_id: Optional[ID_TYPE] = get_fk("process_data.id", True)
    sample_process_id: Optional[ID_TYPE] = get_fk("sample_process.id", True)


class SampleCollection(Entity, table=True, registry=model_registry):
    __tablename__ = "sample__collection"
    __identifying__ = {"sample_id", "collection_id"}
    sample_id: Optional[ID_TYPE] = get_fk("sample.id", True)
    collection_id: Optional[ID_TYPE] = get_fk("collection.id", True)


class Collection(EntityId, table=True, registry=model_registry):
    __tablename__ = "collection"
    label: str
    type: str
    samples: List["Sample"] = Relationship(back_populates="collections", link_model=SampleCollection)
    __identifying__ = {"label", "type"}


class ProcessData(EntityId, table=True, registry=model_registry):
    __tablename__ = "process_data"
    __identifying__ = {"file_name", "file_type"}
    file_name: str
    file_type: str
    raw_data_json: Optional[str]
    sample_processes: List["SampleProcess"] = Relationship(
        back_populates="process_data", link_model=SampleProcessProcessData
    )


class ProcessDetail(EntityId, table=True, registry=model_registry):
    __tablename__ = "process_detail"
    type: str
    technique: str
    processes: List["Process"] = Relationship(back_populates="process_detail")
    __identifying__ = {"type", "technique"}


class Process(EntityId, table=True, registry=model_registry):
    __identifying__ = {"machine_name", "ordering", "timestamp"}
    machine_name: str
    timestamp: datetime
    ordering: int
    process_detail_id: Optional[ID_TYPE] = get_fk("process_detail.id")
    process_detail: ProcessDetail = Relationship(back_populates="processes")
    sample_processes: List["SampleProcess"] = Relationship(back_populates="process")


class Sample(EntityId, table=True, registry=model_registry):
    __tablename__ = "sample"
    label: str
    type: str
    __identifying__ = {"label", "type"}
    sample_processes: List["SampleProcess"] = Relationship(back_populates="sample")
    collections: List[Collection] = Relationship(back_populates="samples", link_model=SampleCollection)


class SampleProcess(EntityId, table=True, registry=model_registry):
    __tablename__ = "sample_process"
    __identifying__ = {"sample_id", "process_id"}
    sample_id: Optional[ID_TYPE] = get_fk("sample.id")
    process_id: Optional[ID_TYPE] = get_fk("process.id")
    sample: "Sample" = Relationship(back_populates="sample_processes")
    process: "Process" = Relationship(back_populates="sample_processes")
    process_data: List[ProcessData] = Relationship(
        back_populates="sample_processes", link_model=SampleProcessProcessData
    )
