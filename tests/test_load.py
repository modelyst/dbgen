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

from pytest import fixture

from dbgen import Attr, Const, Entity, Model, Rel


@fixture
def model():
    model = Model("test_entity")
    entity_c = Entity("c", attrs=[Attr("c_1", identifying=True)])
    entity_d = Entity("d", attrs=[Attr("d_1", identifying=True)])
    entity_e = Entity("e", attrs=[Attr("e_1", identifying=True)])
    entity_b = Entity(
        "b",
        attrs=[Attr("b_1", identifying=True)],
        fks=[Rel('c_id', 'c', identifying=True), Rel('d_id', 'd', identifying=True)],
    )
    entity_a = Entity(
        "a",
        attrs=[Attr("a_1", identifying=True)],
        fks=[Rel('b_id', 'b', identifying=True), Rel('e_id', 'e', identifying=True)],
    )
    model.add([entity_c, entity_d, entity_e, entity_b, entity_a])
    return model


def test_recursive_dependencies_tabdeps(model):
    """Test load.tabdeps() for recursive dependencies."""
    e = model.get("e")
    d = model.get("d")
    c = model.get("c")
    b = model.get("b")
    a = model.get("a")

    val = Const(0)
    load_c = c(c_1=val)
    load_b = b(b_1=val, c_id=load_c, d_id=Const(None))
    load_a = a(insert=True, a_1=val, b_id=load_b, e_id=Const(None))

    assert load_a.tabdeps(model.objs) == ['b', 'c']
    assert load_b.tabdeps(model.objs) == ['b', 'c']
    assert load_c.tabdeps(model.objs) == ['c']

    load_e = e(e_1=val)
    load_d = d(d_1=val)
    load_b = b(insert=True, b_1=val, c_id=load_c, d_id=load_d)
    load_a = a(insert=True, a_1=val, b_id=load_b, e_id=load_e)

    assert load_a.tabdeps(model.objs) == ['c', 'd', 'e']
    assert load_b.tabdeps(model.objs) == ['c', 'd']

    load_e.insert = True
    load_d = d(d_1=val)
    load_b = b(insert=True, b_1=val, c_id=load_c, d_id=load_d)
    load_a = a(insert=True, a_1=val, b_id=load_b, e_id=load_e)

    assert load_a.tabdeps(model.objs) == ['c', 'd']
    assert load_b.tabdeps(model.objs) == ['c', 'd']
