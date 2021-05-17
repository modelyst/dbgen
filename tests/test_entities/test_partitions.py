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

"""Tests Postgresql Partitions"""
from typing import List

from pytest import fixture, mark, raises

from dbgen import (
    JSON,
    JSONB,
    Attr,
    Const,
    Date,
    Decimal,
    Entity,
    Gen,
    Int,
    Model,
    PyBlock,
    Query,
    Rel,
    Text,
    Timestamp,
    Varchar,
)
from dbgen.core.schema import Partition
from dbgen.utils.exceptions import DBgenInvalidArgument, DBgenMissingInfo


@fixture
def model():
    model = Model("test_entity")
    return model


@fixture
def partition_model():
    model = Model("test_entity")
    entity_c = Entity("c", attrs=[Attr("c_1", identifying=True)])
    entity_a = Entity(
        "a",
        attrs=[
            Attr("a_1", identifying=True),
            Attr("a_2", identifying=True, partition_on=True, partition_values=[1, 2]),
            Attr("a_3"),
        ],
        fks=[Rel("c_id", "c", identifying=True)],
    )
    entity_b = Entity("b", attrs=[Attr("b_1")], fks=[Rel("a_id", "a", identifying=True)])
    model.add([entity_c, entity_a, entity_b])

    gen1 = Gen("gen1", loads=[entity_c(insert=True, c_1=Const(1))])
    query_2 = Query(exprs={"c_id": entity_c.id(), "c_1": entity_c["c_1"]()})
    func = lambda x: x + 1
    transform_2 = PyBlock(func, args=[query_2["c_1"]])
    gen2 = Gen(
        "gen2",
        query=query_2,
        transforms=[transform_2],
        loads=[entity_a(insert=True, c_id=query_2["c_id"], a_1=Const(1), a_2=transform_2["out"])],
    )
    query_3 = Query(exprs={"a_id": entity_a.id(), "a_1": entity_a["a_1"]()})
    transform_3 = PyBlock(func, args=[query_3["a_1"]])
    gen3 = Gen(
        "gen3",
        query=query_3,
        transforms=[transform_3],
        loads=[entity_b(insert=True, b_1=transform_3["out"], a_id=query_3["a_id"])],
    )
    model.add([gen1, gen2, gen3])
    return model


@fixture
def sample():
    phase_attr = Attr(
        "phase",
        Text(),
        partition_on=True,
        partition_values=('gas', 'liquid', 'solid'),
        partition_default=True,
    )
    return Entity(
        "sample",
        attrs=[Attr("label", Text(), identifying=True), phase_attr],
    )


def test_basic_partition(model: Model, sample: Entity):
    model.add([sample])


def test_basic_type_validation():
    """Tests basic type validation for partitioned values"""
    with raises(DBgenInvalidArgument):
        Attr(
            "phase",
            Int(),
            partition_on=True,
            partition_values=('gas', 'liquid', 'solid'),
            partition_default=True,
        )

    with raises(DBgenInvalidArgument):
        Attr(
            "phase",
            Int(),
            partition_on=True,
            partition_values=('gas', 'liquid', 'solid'),
            partition_default=True,
        )


def test_missing_val_or_default():
    """Assertion error when no default and no values are selected"""
    with raises(DBgenMissingInfo):
        Attr(
            "phase",
            Text(),
            partition_on=True,
            partition_default=False,
        )
    Attr(
        "phase",
        Text(),
        partition_on=True,
        partition_default=True,
    )
    Attr("phase", Text(), partition_on=True, partition_default=False, partition_values=("Test"))


def test_simple_entity():
    """Can have at most 1 partitioned column"""
    attr_b = Attr("B", partition_on=True)
    with raises(DBgenInvalidArgument):
        Entity("test", attrs=[Attr("A", partition_on=True), attr_b])
    test = Entity("test", attrs=[Attr("A", partition_on=False), attr_b])
    assert test.partition_attr == attr_b
    test = Entity("test", attrs=[Attr("A")])
    assert test.partition_attr is None


@mark.parametrize(
    "dtype,vals",
    ((Text(), ("cat1", "cat2", "cat3")), (Varchar(), ("cat1", "cat2", "cat3")), (Int(), (1, 2, 3))),
    ids=["text", "varchar", "int"],
)
def test_simple_entity_create(dtype, vals):
    # Check partition_by and Primary key syntax
    attr_b = Attr("B", dtype, partition_on=True, partition_values=vals)
    test = Entity("test", attrs=[Attr("A", partition_on=False, index=True), attr_b])
    statements = test.create()
    print(";\n".join(statements))
    assert "PARTITION BY LIST (\"b\")" in statements[0]
    assert "PRIMARY KEY (test_id,\"b\")" in statements[0]
    # Check that no partition exists for non partitioned tables
    test = Entity("test", attrs=[Attr("A", partition_on=False), Attr("B")])
    create_str, *_ = test.create()
    assert "PARTITION BY" not in create_str


@mark.parametrize("dtype", (Timestamp, Date, Decimal, JSONB, JSON))
def test_bad_dtype_partition(dtype):
    """Non-Allowed Partition dtypes"""
    with raises(NotImplementedError):
        Attr("B", dtype, partition_on=True)


def test_complex_model_instantiation(partition_model):
    assert len(partition_model.objs) == 6

    entity_c = partition_model.get("c")
    entity_a = partition_model.get("a")
    entity_a_1 = partition_model.get("a", 1)
    entity_a_2 = partition_model.get("a", 2)
    # entity_a_default = partition_model.get("a", default=True)
    assert isinstance(entity_a_1, Partition)
    assert isinstance(entity_a_2, Partition)
    assert isinstance(entity_a, Entity)
    # Test default entity
    # assert entity_a_1 != entity_a_default
    # assert entity_a_2 != entity_a_default
    # assert entity_a != entity_a_default
    func = lambda x: x + 1
    query_4 = Query(
        exprs={
            "a_id": entity_a_1.id(),
            "c_id": entity_c.id(partition_model.make_path("c", [entity_a_1.r("c_id")])),
            "a_1": entity_a_1["a_1"](),
        },
        basis=[entity_a_1],
    )
    transform_4 = PyBlock(func, args=[query_4["a_1"]])
    gen4 = Gen(
        "gen4",
        query=query_4,
        transforms=[transform_4],
        loads=[entity_a_2(insert=True, c_id=query_4["c_id"], a_1=transform_4["out"], a_2=Const(2))],
    )
    query_5 = Query(
        exprs={
            "a_id": entity_a_1.id(),
            "c_id": entity_c.id(partition_model.make_path("c", [entity_a_1.r("c_id")])),
            "a_1": entity_a_1["a_1"](),
        },
        basis=[entity_a_1],
    )
    func_5 = lambda x: x + 2
    transform_5 = PyBlock(func_5, args=[query_5["a_1"]])
    gen5 = Gen(
        "gen5",
        query=query_5,
        transforms=[transform_5],
        loads=[entity_a_2(insert=True, c_id=query_5["c_id"], a_1=transform_5["out"], a_2=Const(2))],
    )
    partition_model.add([gen4, gen5])
    order = {g.name: i for i, g in enumerate(partition_model.ordered_gens())}
    assert all([order["gen1"] < order[f"gen{i}"] for i in (2, 3, 4, 5)])
    assert all([order["gen3"] > order[f"gen{i}"] for i in (2, 4, 5)])
    assert order["gen4"] > order["gen2"] and order["gen5"] > order["gen2"]


def test_get_partitions(partition_model):
    entity_a = partition_model.get("a")
    entity_a_1 = partition_model.get("a", 1)
    entity_a_2 = partition_model.get("a", 2)
    partitions_a = entity_a.get_all_partitions()
    assert len(partitions_a) == 3
    assert partitions_a[1] == entity_a_1
    assert partitions_a[2] == entity_a_2
    assert len(partition_model.objs) == 6
    tabnames = ["a", "b", "c", "a__a_2__1", "a__a_2__2", "a__a_2__default"]
    print(partition_model.objs)
    assert all(map(lambda x: x in partition_model.objs, tabnames))


def test_query_dep(partition_model):
    entity_a_1 = partition_model.get("a", 1)
    entity_c = partition_model.get("c")
    query = Query(
        exprs={
            "a_id": entity_a_1.id(),
            "c_id": entity_c.id(partition_model.make_path("c", [entity_a_1.r("c_id")])),
            "a_1": entity_a_1["a_1"](),
        },
        basis=[entity_a_1],
    )
    assert list(sorted(set(query.allobj()))) == ['a__a_2__1', 'c']


load_dep_answers = (
    (None, ["a", "a__a_2__1", "a__a_2__2", "a__a_2__default"]),
    (1, ["a", "a__a_2__1"]),
    (2, ["a", "a__a_2__2"]),
)


@mark.parametrize("value,answer", load_dep_answers, ids=["a", "a_1", "a_2"])
def test_load_dep(partition_model: 'Model', value: int, answer: List[str]):
    entity_a = partition_model.get("a", value)
    entity_c = partition_model.get("c")
    query = Query(
        exprs={
            "a_id": entity_a.id(),
            "c_id": entity_c.id(partition_model.make_path("c", [entity_a.r("c_id")])),
            "a_1": entity_a["a_1"](),
        },
        basis=[entity_a],
    )
    # Insertion Load
    load_a = entity_a(insert=True, c_id=query["c_id"], a_1=Const(1), a_2=Const(1), a_3=Const(1))
    newtabs = load_a.newtabs(partition_model.objs)
    assert sorted(newtabs) == answer
    newcols = load_a.newcols(partition_model.objs)
    newcols_answer = []
    for entity in answer:
        newcols_answer.extend([f"{entity}.{col}" for col in ["a_1", "a_2", "c_id", "a_3"]])
    assert sorted(newcols) == sorted(newcols_answer)
    tabdeps = load_a.tabdeps(partition_model.objs)
    assert tabdeps == ['c']

    # Non-Insertion Load
    pk_name = "a" if value is None else f"a__a_2__{value}"
    load_kwargs = {pk_name: query["a_id"]}
    load_a = entity_a(insert=False, a_3=Const(1), **load_kwargs)
    newtabs = load_a.newtabs(partition_model.objs)
    assert newtabs == []
    newcols = load_a.newcols(partition_model.objs)
    newcols_answer = []
    for entity in answer:
        newcols_answer.extend([f"{entity}.{col}" for col in ["a_3"]])
    assert sorted(newcols) == sorted(newcols_answer)
    tabdeps = load_a.tabdeps(partition_model.objs)
    assert sorted(tabdeps) == answer
