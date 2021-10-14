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

# from datetime import datetime
# from hashlib import md5
# from uuid import UUID

# import pytest
# from pydantic import ValidationError
# from sqlalchemy import func
# from sqlmodel import Session, select

# from tests.example.model import (
#     Collection,
#     Process,
#     ProcessDetail,
#     Sample,
#     SampleCollection,
#     SampleProcess,
# )

# pytestmark = pytest.mark.skip


# def test_basic_instantiation(session: Session, make_db):
#     sample = Sample(label=123, type="jcap")
#     assert str(sample.id) == "96af4bbd-b78d-6407-1f88-3be95766867c"
#     process_detail = ProcessDetail(type="eche", technique="CA6")
#     assert str(process_detail.id) == "e807214b-7c9d-8c92-fd11-de11a27f4475"
#     process = Process(
#         machine_name="hte-anec-1",
#         ordering=0,
#         timestamp=datetime(2021, 8, 31, 12, 13, 37, 364598),
#     )
#     assert str(process.id) == "8c1be0c8-887c-1987-109b-3367b45c7b81"
#     process.process_detail = process_detail
#     assert process in process_detail.processes
#     sample_process = SampleProcess(
#         sample_id=sample.id, process_id=process.id, sample=sample, process=process
#     )
#     assert sample_process.id == UUID("60e24ac6-01f0-1734-cda7-fd722fd09900")
#     session.add(sample_process)
#     result = session.exec(select(func.count(SampleProcess.id))).one()  # type: ignore
#     assert result == 1
#     result = session.exec(select(SampleProcess.id)).one()
#     assert result == UUID("60e24ac6-01f0-1734-cda7-fd722fd09900")
#     process_in_db = session.exec(select(Process).where(Process.id == process.id)).one()
#     assert process_in_db.process_detail == process_detail
#     process_in_db, process_detail_in_db = session.exec(
#         select(Process, ProcessDetail)
#         .join(ProcessDetail)
#         .where(Process.id == process.id)
#     ).one()
#     assert process_detail_in_db == process_detail


# def test_empty_db(session, make_db):
#     result = session.exec(select(func.count(SampleProcess.id))).first()
#     assert result == 0


# def test_empty_db_new(session, make_db):
#     result = session.exec(select(func.count(SampleProcess.id))).first()
#     assert result == 0


# def test_validation(make_db):
#     with pytest.raises(ValidationError):
#         Sample(
#             label=123,
#         )


# def test_complex_insert(session, make_db):
#     SAMPLE_COUNT = 1000
#     samples = [Sample(label=i, type="test") for i in range(SAMPLE_COUNT)]
#     collection = Collection(label="test", type="test", samples=samples)
#     session.add(collection)
#     result = session.exec(select(Sample).where(Sample.label == "1"))
#     sample_out = result.one()
#     assert collection in sample_out.collections
#     label, count = session.exec(
#         select(Collection.label, func.count(1))
#         .join(SampleCollection)
#         .group_by(Collection.id)
#     ).one()

#     assert count == SAMPLE_COUNT
#     assert label == "test"
#     session.rollback()
#     out = session.exec(select(func.count(1)).select_from(Sample)).one()
#     assert out == 0
#     process = Process(
#         machine_name="hte-anec-1",
#         ordering=0,
#         timestamp=datetime(2021, 8, 31, 12, 13, 37, 364598),
#     )
#     process.sample_processes = [
#         SampleProcess(sample_id=sample.id, sample=sample, process_id=process.id)
#         for sample in [Sample(label=i, type="test") for i in range(5)]
#     ]

#     session.add(process)
#     out = session.exec(select(func.count(1)).select_from(Sample)).one()
#     assert out == 5
#     out = session.exec(
#         select(Process.id, func.count(1)).join(SampleProcess).group_by(Process.id)
#     ).one()
#     assert out[1] == 5
#     out = session.exec(select(func.count(1)).select_from(Sample)).one()
#     assert out == 5
#     sample_process = SampleProcess(sample_id=samples[0].id, process_id=process.id)
#     session.exec(
#         select(SampleProcess).where(SampleProcess.id == sample_process.id)
#     ).one()


# def fake_hasher(entity) -> UUID:
#     return UUID(
#         md5(
#             entity.json(include={k: ... for k in entity.__identifying__}).encode(
#                 "utf-8"
#             )
#         ).hexdigest()
#     )


# def test_hasher(make_db):
#     sample = Sample(label=1, type=1)
#     assert sample.id == fake_hasher(sample)
#     process = Process(machine_name="test", timestamp=datetime(1994, 9, 30), ordering=0)
#     assert process.id == fake_hasher(process)
#     sample_process = SampleProcess(sample_id=sample.id, process_id=process.id)
#     assert sample_process.id == fake_hasher(sample_process)
#     assert not sample_process.deleted
#     pd = ProcessDetail(type="eche", technique="CA6")
#     process.process_detail = pd
#     assert process.id == fake_hasher(process)
#     process.ordering = 2
#     with pytest.raises(ValueError):
#         process.validate_hash()
