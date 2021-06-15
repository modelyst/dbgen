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

"""Tests the naming of paths for Query Object"""
from pytest import fixture

from dbgen.example.main import make_model


@fixture
def model():
    return make_model()


def test_simple_unnamed_path(model):
    """Unnamed paths with the same start and end should be equivalent"""
    ElecComp = model.get("electrode_composition")
    path_one = model.make_path("element", [ElecComp.r("element"), ElecComp.r("electrode")])
    path_two = model.make_path("element", [ElecComp.r("element"), ElecComp.r("electrode")])
    assert path_one._from().aliases() == path_two._from().aliases()
    assert path_one == path_two
    assert path_one.hash == path_two.hash


def test_simple_named_path(model):
    ElecComp = model.get("electrode_composition")
    Elem = model.get("element")
    path_one = model.make_path("element", [ElecComp.r("element"), ElecComp.r("electrode")], name="Ag")
    path_two = model.make_path("element", [ElecComp.r("element"), ElecComp.r("electrode")], name="Cu")
    assert path_one._from().aliases() != path_two._from().aliases()
    assert path_one != path_two
    assert path_one.hash != path_two.hash
    elem_one = Elem["symbol"](path_one)
    mass_one = Elem["mass"](path_one)
    elem_two = Elem["symbol"](path_two)
    mass_two = Elem["mass"](path_two)
    # query = Query(
    #     {"electrode": Elec["composition"](), "mass_one": mass_one, "mass_two": mass_two},
    #     basis=[Elec],
    #     constr=AND([EQ(elem_one, Literal("Ag")), EQ(elem_two, Literal("Cu"))]),
    # )
    assert repr(elem_one).startswith("PathAttr<element(Ag") and repr(elem_one).endswith(").symbol>")
    assert repr(mass_one).startswith("PathAttr<element(Ag") and repr(mass_one).endswith(").mass>")
    assert repr(elem_two).startswith("PathAttr<element(Cu") and repr(elem_two).endswith(").symbol>")
    assert repr(mass_two).startswith("PathAttr<element(Cu") and repr(mass_two).endswith(").mass>")
