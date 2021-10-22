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

import pytest
from sqlalchemy.orm import aliased
from sqlmodel import func, select

import tests.example.model as model
from dbgen.core.args import Arg
from dbgen.core.node.query import BaseQuery, _get_select_keys


@pytest.fixture
def complex_statement():
    statement = (
        select(model.Sample.id, model.Sample.label.label("sample_label"))
        .join(model.SampleCollection)
        .join(model.Collection)
        .join(model.SampleProcess)
        .where(model.Collection.label == "1")
        .group_by(model.Collection.id)
        .order_by(model.Collection.type)
        .having(func.count(model.SampleProcess.id) > 1)
    )
    return statement


basic_statement = select(model.Sample.id, model.Sample.label, model.Collection.label.label("col_label"))


def test_statement_dependencies(complex_statement):

    expected_cols = (
        f"{model.Sample.__fulltablename__}.id",
        f"{model.Sample.__fulltablename__}.label",
        f"{model.Collection.__fulltablename__}.label",
        f"{model.Collection.__fulltablename__}.id",
        f"{model.Collection.__fulltablename__}.type",
        f"{model.SampleProcess.__fulltablename__}.id",
        f"{model.SampleCollection.__fulltablename__}.sample_id",
        f"{model.SampleCollection.__fulltablename__}.collection_id",
        f"{model.SampleProcess.__fulltablename__}.sample_id",
    )
    expected_tables = (
        model.Sample.__fulltablename__,
        model.Collection.__fulltablename__,
        model.SampleCollection.__fulltablename__,
        model.SampleProcess.__fulltablename__,
    )
    query = BaseQuery.from_select_statement(complex_statement)
    dep = query.dependency
    assert set(expected_cols) == dep.columns_needed
    assert set(expected_tables) == dep.tables_needed


statements = [
    (select(model.Sample), {"id", "created_at", "gen_id", "label", "type"}),
    (
        select(aliased(model.Sample, name="test")),
        {"id", "created_at", "gen_id", "label", "type"},
    ),
    (select(model.Collection), {"id", "created_at", "gen_id", "label", "type"}),
    (select(model.Sample), {"id", "created_at", "gen_id", "label", "type"}),
    (
        select(model.Sample, model.Collection.id.label("collection_id")),
        {"id", "created_at", "gen_id", "label", "type", "collection_id"},
    ),
]


@pytest.mark.parametrize(["statement", "answer"], statements)
def test_select_keys(statement, answer):
    select_keys = _get_select_keys(statement)
    assert set(select_keys) == answer


ambiguous_statements = [
    select(model.Sample.id, model.Collection.id),
    select(model.Sample.id.label("test"), model.Collection.id.label("test")),
    select(
        aliased(model.Sample, name="alias_sample").id.label("test"),
        model.Collection.id.label("test"),
    ),
]


@pytest.mark.parametrize("statement", ambiguous_statements)
def test_ambiguous_column_names(statement):
    """Test that ambiguous column names are rejected."""
    with pytest.raises(ValueError, match="Ambiguous Column"):
        _get_select_keys(statement)


def test_query_object():
    """Simple query object instantiation"""
    statement = select(model.Sample.id, model.Sample.label)
    query = BaseQuery.from_select_statement(statement)

    assert query.dependency.tables_needed == {model.Sample.__fulltablename__}
    assert query.dependency.columns_needed == {
        f"{model.Sample.__fulltablename__}.{col.name}" for col in (model.Sample.id, model.Sample.label)
    }

    with pytest.raises(ValueError, match="Ambiguous"):
        BaseQuery.from_select_statement(ambiguous_statements[0])


def test_base_query():
    base_query = BaseQuery.from_select_statement(basic_statement)
    assert base_query.dependency.columns_yielded == set()
    assert base_query.dependency.tables_yielded == set()
    assert base_query.dependency.columns_needed == {
        "public.sample.id",
        "public.sample.label",
        "public.collection.label",
    }
    assert base_query.dependency.tables_needed == {"public.sample", "public.collection"}
    assert set(base_query.outputs) == {"label", "id", "col_label"}
    assert base_query.query == str(basic_statement)

    assert base_query.dict(exclude={"tester", "dependency", "inputs"}) == {
        "query": str(basic_statement),
        "outputs": ["id", "label", "col_label"],
    }
    assert isinstance(base_query.hash, str)
    assert isinstance(base_query.dict(), dict)

    for arg_name in ("label", "id", "col_label"):
        arg_obj = base_query[arg_name]
        assert isinstance(arg_obj, Arg)

    with pytest.raises(AssertionError):
        base_query["non_existent"]
