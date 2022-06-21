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

from typing import cast

import pytest
import sqlalchemy
from sqlmodel import Session, func, select, text

from dbgen.core.args import Constant
from dbgen.core.etl_step import ETLStep
from dbgen.core.metadata import ETLStepEntity, ETLStepRunEntity, RunEntity, meta_registry
from dbgen.core.model import Model
from dbgen.core.node.query import BaseQuery
from dbgen.core.node.transforms import PythonTransform
from tests.example.full_model import Parent
from tests.example_functions import binary_lambda


@pytest.mark.database
def test_meta_registry(connection, recreate_meta):
    """assert creating metadb doesn't create Entity DBs"""
    with Session(connection) as sess:
        sess = cast(Session, sess)
        result = sess.exec(select(RunEntity.id)).one_or_none()
        assert result is None
        with pytest.raises(sqlalchemy.exc.ProgrammingError):
            sess.exec(select(Parent))


@pytest.mark.database
def test_run_insertion_and_update(connection, recreate_meta):
    run_1 = RunEntity(status="initialized")

    with Session(connection) as sess:
        sess.add(run_1)
        sess.commit()
        sess.refresh(run_1)
        assert run_1.id == 1
        run_1.status = "failed"
        sess.commit()
    with Session(connection) as sess:
        run_id, status = sess.exec(select(RunEntity.id, RunEntity.status)).one_or_none()
        assert run_id == 1
        assert status == "failed"
        run_2 = RunEntity(status="initialized")
        sess.add(run_2)
        run_count = sess.exec(select(func.count(RunEntity.id))).one()
        assert run_count == 2
        sess.refresh(run_2)
        assert run_2.id == 2


query = BaseQuery(
    outputs=["label"],
    query="select 1 as label;",
    dependency={"tables_needed": {"test"}},
)
pb = PythonTransform(inputs=[Constant(val=1), Constant(val=2)], function=binary_lambda)
etl_steps = [
    ETLStep(name="test"),
    ETLStep(name="test_etl_step", tags=["a", "b", "c"]),
    ETLStep(
        name="test_etl_step",
        transforms=[pb],
    ),
    ETLStep(
        name="test_etl_step",
        transforms=[pb],
        loads=[Parent.load(insert=True, label=pb["out"])],
    ),
    ETLStep(
        name="test_etl_step",
        extract=query,
        transforms=[pb],
        loads=[Parent.load(insert=True, label=pb["out"])],
    ),
]


@pytest.mark.database
@pytest.mark.parametrize("etl_step", etl_steps)
def test_etl_step_insertion(connection, recreate_meta, etl_step: ETLStep):
    """Test that etl_steps can be inserted into the database and deserialized upon querying."""
    etl_step_row = etl_step._get_etl_step_row()
    with Session(connection) as sess:
        sess.merge(etl_step_row)
        etl_step_count = sess.exec(select(func.count(ETLStepEntity.id))).one_or_none()
        assert etl_step_count == 1
        etl_step_dict = sess.exec(select(ETLStepEntity.etl_step_json)).one_or_none()
        query_etl_step = ETLStep.deserialize(etl_step_dict)
        assert query_etl_step._id_dict() == etl_step._id_dict()
        assert query_etl_step.hash == etl_step.hash


@pytest.mark.database
def test_etl_step_run_insertion(connection, recreate_meta):
    etl_step = etl_steps[0]._get_etl_step_row()
    run = RunEntity()
    with Session(connection) as sess:
        sess.add(run)
        sess.add(etl_step)
        sess.commit()
        sess.refresh(run)
        etl_step_run = ETLStepRunEntity(run_id=run.id, etl_step_id=etl_step.id)
        sess.add(etl_step_run)
        sess.commit()


def test_current_run_created(connection, recreate_meta):
    connection.commit()
    drop_metadata = Model(name='test').drop_metadata
    check_current_run_exists = lambda: connection.execute(text('select * from dbgen_log.current_run;'))
    check_current_run_exists()
    drop_metadata(connection, meta_registry.metadata)
    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        check_current_run_exists()
    connection.rollback()
    meta_registry.metadata.drop_all(connection)
    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        check_current_run_exists()
    connection.rollback()
    meta_registry.metadata.create_all(connection)
    check_current_run_exists()
    connection.commit()
    Model(name='test').meta_registry.metadata.drop_all(connection)
    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        check_current_run_exists()


def test_sync(sql_engine):
    meta_registry.metadata.drop_all(sql_engine)
    check_current_run_exists = lambda conn: conn.execute(text('select * from dbgen_log.current_run;'))
    with sql_engine.connect() as connection:
        with pytest.raises(sqlalchemy.exc.ProgrammingError):
            check_current_run_exists(connection)

    Model(name='test').sync(sql_engine, sql_engine, build=True)
    with sql_engine.connect() as connection:
        check_current_run_exists(connection)
