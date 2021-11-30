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
from typing import Optional
from uuid import UUID

import pytest
from hypothesis import given
from pydantic import ValidationError
from pydasher import hasher
from sqlalchemy import text
from sqlalchemy.orm.decl_api import registry
from sqlmodel import Field, select

from dbgen.core.args import Const
from dbgen.core.entity import BaseEntity, Entity, EntityMetaclass, create_entity
from dbgen.core.node.load import LoadEntity
from dbgen.exceptions import InvalidArgument
from tests.strategies.entity import example_entity, fill_required_fields

id_field = Field(
    default=None,
    primary_key=True,
    sa_column_kwargs={"autoincrement": False, "unique": True},
)


def test_entity_hash(clear_registry):
    class DummyEntity(BaseEntity):
        label: str
        type: str
        details: dict = {}
        __identifying__ = {"label", "type"}

    class DummyEntityV2(DummyEntity):
        new_key: int
        __identifying__ = {"new_key"}

    sample = DummyEntity(label="test_label", type="test_type")
    sample_v2_1 = DummyEntityV2(label="test_label", type="test_type", new_key=3.0)
    sample_v2_2 = DummyEntityV2(label="test_label", type="test_type", new_key=3)
    assert all(map(lambda x: x in sample_v2_1._id_dict()['_value'], ("new_key", "label", "type")))
    assert sample_v2_1 == sample_v2_2
    assert isinstance(sample.hash, str)

    assert hasher(sample) == sample.hash
    assert sample.parse_obj(sample.dict()).hash == sample.hash


def test_entity_inheritance(clear_registry):
    """Make sure that __identifying__, and the hash fields are being passed down through inheritance."""

    class Grandparent(BaseEntity):
        grandparent: str
        __identifying__ = {"grandparent"}
        _hashinclude_ = {"x"}
        _hashexclude_ = {"y"}

    class Parent(Grandparent):
        name: str
        parent: str
        __identifying__ = {"parent"}

    class Child(Parent):
        name: str
        child: int
        __identifying__ = {"child", "name"}
        _hashexclude_ = {"z"}

        class Config:
            json_encoders = {Parent: lambda x: str(x)}

    kwargs = dict(name="steve", child=1, parent="parent", grandparent="grandparent")
    child = Child(**kwargs)
    assert child._hashinclude_ == {"x", "grandparent", "parent", "child", "name"}
    assert child._hashexclude_ == {"y", "z"}
    assert child._id_dict() == {
        '_type': 'basemodel',
        "_base_model_type": "tests.test_entity.Child",
        '_value': kwargs,
    }
    assert Parent in child.__config__.json_encoders
    assert UUID in child.__config__.json_encoders


def test_load_entity(clear_registry):
    class TestSample(BaseEntity, table=True):
        __tablename__ = "test_sample"
        id_col: Optional[UUID] = id_field
        label: str
        type: str
        last_updated: datetime = Field(default_factory=datetime.now)
        __identifying__ = {"label", "type"}

    sample = TestSample(label="1", type="test")
    assert "last_updated" in sample.dict()
    assert "last_updated" not in sample._id_dict()
    columns = sample._columns()
    assert len(columns) == 4
    test_load_entity = TestSample._get_load_entity()
    assert test_load_entity.name == "test_sample"
    assert test_load_entity.primary_key_name == "id_col"
    assert test_load_entity.identifying_attributes == {"label", "type"}
    assert test_load_entity.identifying_foreign_keys == set()
    assert sample.__fields_set__ == {"label", "type"}


def test_basic_load(clear_registry):
    class Dummy4(BaseEntity, table=True):
        id: Optional[UUID] = id_field
        key_1: int
        key_2: str
        __identifying__ = {"key_1"}

    load = Dummy4.load(key_1=Const(1))
    assert load.load_entity.name == "dummy4"
    assert "key_1" in load.inputs
    assert "key_2" not in load.inputs


def test_basic_parent_child_load(clear_registry):
    class Tester(BaseEntity):
        id: Optional[UUID] = id_field
        deleted: bool = False

    class DummyParent(Tester, table=True):
        key_1: int
        key_2: str
        __identifying__ = {"key_1"}

    class DummyChild(Tester, table=True):
        dummy_parent_id: Optional[UUID] = Field(foreign_key="dummyparent.id")
        __identifying__ = {"dummy_parent_id"}

    load_entity = DummyChild._get_load_entity()
    assert load_entity.name == "dummychild"
    assert load_entity.identifying_foreign_keys == {"dummy_parent_id"}
    assert load_entity.identifying_attributes == set()

    parent_load = DummyParent.load(key_1=Const(1))
    child_load = DummyChild.load(insert=True, dummy_parent_id=parent_load)
    assert (
        "dummy_parent_id" in child_load.inputs
        and child_load.inputs["dummy_parent_id"] == parent_load["dummyparent_id"]
    )
    child_load = DummyChild.load(insert=True, dummy_parent_id=Const(None))
    assert "dummy_parent_id" in child_load.inputs and child_load.inputs["dummy_parent_id"] == Const(None)


def test_identifying_info_validation(clear_registry):
    with pytest.raises(ValueError, match="Invalid Entity Class Definition."):

        class Test(BaseEntity):
            type: str
            __identifying__ = {"label"}

    class TestParent(BaseEntity):
        type: str
        label: str
        __identifying__ = {"label"}

    with pytest.raises(ValueError, match="Invalid Entity Class Definition."):

        class TestChild(TestParent):
            plate_id: int
            __identifying__ = {"nonexistent"}


def test_registry(clear_registry):
    start = len(BaseEntity.metadata.tables)
    assert len(BaseEntity.metadata.tables) == 0

    class TableWithRegistry(BaseEntity, table=True):
        id: UUID = id_field

    class Parent(BaseEntity, table=True):
        id: UUID = id_field

    assert TableWithRegistry.__fulltablename__ in BaseEntity.metadata.tables
    assert Parent.__fulltablename__ in BaseEntity.metadata.tables
    assert len(BaseEntity.metadata.tables) - start == 2


@given(entity_class=example_entity(fks={}))
def test_entity_hypo(entity_class: EntityMetaclass):
    instance: BaseEntity = fill_required_fields(entity_class)
    assert instance.__fulltablename__ in instance.metadata.tables
    load_entity = instance._get_load_entity()
    assert load_entity.name == instance.__tablename__
    assert (
        load_entity.identifying_foreign_keys.union(load_entity.identifying_attributes)
        == instance.__identifying__
    )


@given(example_entity(fks={"parent_id": "parent.id"}))
def test_entity_fks_hypo(entity_class: EntityMetaclass):
    instance: BaseEntity = fill_required_fields(entity_class)
    assert len(instance.__fields__) >= 3
    assert instance.__fulltablename__ in instance.metadata.tables
    load_entity = instance._get_load_entity()
    assert load_entity.name == instance.__tablename__
    assert instance.__identifying__.issuperset(load_entity.identifying_attributes)
    assert instance.__identifying__.issuperset(load_entity.identifying_foreign_keys)


@given(
    example_entity(
        fks={},
        attrs={"a": (str, "hello world"), "b": (int,)},
        draw_attrs=False,
    )
)
def test_extra_attrs(entity_class: EntityMetaclass):
    with pytest.raises(ValidationError):
        entity_class()
    instance = entity_class(b=99)
    assert instance.a == "hello world"
    assert instance.b == 99
    instance = entity_class(a=2, b=99)
    assert instance.a == "2"
    assert instance.b == 99


def test_dbgen_id_table(clear_registry):
    class TestEntity(Entity, table=True):
        __tablename__ = "tester"
        __identifying__ = {"label", "type"}
        _hashinclude_ = __identifying__
        label: str
        type: str
        non_id: str

    sample_1 = TestEntity(label=1, type="test_type", non_id="1")
    sample_2 = TestEntity(label="1", type="test_type", non_id="2")
    sample_3 = TestEntity(label="3", type="test_type_3", non_id="2")
    sample_4 = TestEntity(label=3, type="test_type_3", non_id="2")
    assert isinstance(sample_1.hash, str)

    assert hasher(sample_1) == sample_1.hash
    assert sample_1.parse_obj(sample_1.dict()).hash == sample_1.hash
    assert sample_1 == sample_2
    assert sample_1 != sample_3
    assert sample_2 != sample_3
    assert sample_3 == sample_4


def test_assert_identifying_column(clear_registry):
    class Dummy(Entity, table=True):
        name: str

    assert Dummy._is_table


def test_is_table_attr(clear_registry):
    """Test the Entity._is_table attribute correct indicates a Entity has table=True."""

    class Dummy(Entity, table=False):
        name: str

    class DummyChild(Dummy):
        pass

    class DummyChildTable(Dummy, table=True):
        pass

    assert not Dummy._is_table
    assert not DummyChild._is_table
    assert DummyChildTable._is_table


def test_validation_of_table_inheritance(clear_registry):
    """Test error thrown when table=True inherits from table=True"""

    class Dummy(Entity, table=True):
        name: str

    class DummyChild(Dummy):
        pass

    with pytest.raises(ValueError):

        class BadDummyGrandChild(DummyChild, table=True):
            pass

    class DummyGrandChild(DummyChild, table=False):
        pass


def test_load_entity_validation(clear_registry):
    """Test error thrown when you get load_entity from non-table Entity"""

    class Dummy(Entity, table=True):
        name: str

    assert isinstance(Dummy._get_load_entity(), LoadEntity)

    class DummyChild(Dummy):
        pass

    for func in (DummyChild._get_load_entity, DummyChild._columns):
        with pytest.raises(ValueError):
            func()


def test_multiple_primary_keys_error(clear_registry):
    """Test error thrown when you Entities have multiple primary keys"""

    with pytest.raises(NotImplementedError):

        class Dummy(BaseEntity, table=True):
            id_1: str = Field(None, primary_key=True)
            id_2: str = Field(None, primary_key=True)

        Dummy._get_load_entity()


def test_clear_registry_method(clear_registry):
    """Test that tables with the same name error and Entity.clear_registry works."""
    assert len(BaseEntity.metadata.tables) == 0

    class DummyV1(Entity, table=True):
        __tablename__ = "dummy"

    class DummyV2(Entity, table=True):
        __tablename__ = "dummy_v2"

    assert len(BaseEntity.metadata.tables) == 2
    with pytest.raises(ValueError):

        class Dummy(Entity, table=True):
            __tablename__ = "dummy"

    class DummyV3(Entity, table=True):
        __tablename__ = "dummy_v3"

    assert len(BaseEntity.metadata.tables) == 3
    BaseEntity.clear_registry()
    assert len(BaseEntity.metadata.tables) == 0

    class DummyV4(Entity, table=True):
        __tablename__ = "dummy"


def test_duplicate_table_name(clear_registry):
    """Test that tables with the same name error and Entity.clear_registry works."""
    assert len(BaseEntity.metadata.tables) == 0
    table_name = "my_table"

    class Table1(Entity, table=True):
        __tablename__ = table_name
        __schema__ = "public"

    class Table2(Entity, table=True):
        __tablename__ = table_name
        __schema__ = "test"

    Table1._get_load_entity()
    Table2._get_load_entity()
    with pytest.raises(ValueError):

        class Table3(Entity, table=True):
            __tablename__ = table_name
            __schema__ = "public"

        # Table3._get_load_entity()


@pytest.mark.database
def test_registry_and_schema_interaction(connection, clear_registry):
    """Add three to various schema and connect to them through a custom registry."""
    registry_1 = registry()
    metadata = registry_1.metadata
    for schema in ("test", "public"):

        class DummyV1(Entity, table=True, registry=registry_1):
            __tablename__ = "dummy"
            __schema__ = schema

        class DummyV2(Entity, table=True, registry=registry_1):
            __tablename__ = "dummy_v2"
            __schema__ = schema

        class PublicTable(Entity, table=True, registry=registry_1):
            __tablename__ = "my_table"
            __schema__ = "public"

        class TestTable(Entity, table=True, registry=registry_1):
            __tablename__ = "my_table"
            __schema__ = "test"

        connection.execute(text('create schema if not exists test;'))
        metadata.create_all(connection)
        tables = ("dummy", "dummy_v2")
        assert len(BaseEntity.metadata.tables) == 0
        connection.commit()
        assert len(metadata.tables) == 4
        tables = map(lambda x: f"{schema}.{x}", tables)
        assert all(map(lambda x: x in metadata.tables, tables)), metadata
        assert "public.my_table" in metadata.tables
        assert "test.my_table" in metadata.tables
        metadata.drop_all(connection)
        metadata.clear()
        registry_1.dispose()
        connection.commit()


@pytest.mark.database
def test_create_entity(connection, session, clear_registry):
    registry_1 = registry()
    Dummy = create_entity(
        "Dummy",
        {"id": (Optional[int], Field(None, primary_key=True)), "name": str},
        identifying={"name"},
        table=True,
        registry=registry_1,
    )
    dummy = Dummy(
        name="test",
    )
    assert dummy.name == "test"
    assert dummy._id_dict() == {
        '_value': {"name": "test"},
        '_type': 'basemodel',
        "_base_model_type": "dbgen.core.entity.Dummy",
    }
    registry_1.metadata.create_all(connection)
    connection.commit()
    session.add(dummy)
    session.commit()
    session.refresh(dummy)

    queried_dummy = session.exec(select(Dummy).where(Dummy.id == dummy.id)).first()
    assert queried_dummy.name == dummy.name
    assert queried_dummy.id == dummy.id
    assert queried_dummy.hash == dummy.hash
    session.delete(dummy)
    session.commit()
    queried_dummy = session.exec(select(Dummy).where(Dummy.id == dummy.id)).first()
    assert not queried_dummy

    DummyChild = create_entity("DummyChild", {"child_name": str}, base=Dummy, identifying={"child_name"})
    DummyChild(
        name="test",
        child_name=1,
    )
    assert "child_name" in DummyChild._hashinclude_
    assert "name" in DummyChild._hashinclude_


@pytest.mark.parametrize(
    'kwarg_name',
    (
        'all_id',
        'all_ids',
        '__identifying_',
        '_hash_include_',
        '_hash_exclude_',
        'is_table',
    ),
)
def test_invalid_kwarg_names(kwarg_name):
    kwargs = {}
    kwargs[kwarg_name] = None
    with pytest.raises(InvalidArgument):

        class Test(Entity, **kwargs):
            pass
