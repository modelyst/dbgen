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

# Internal Modules
from dbgen import Attr, Decimal, Entity, Model, Rel, Varchar

from .generators import add_generators  # type: ignore

emp_rels = [Rel("department"), Rel("manager", "employee")]
emp = Entity(
    name="employee",
    desc="an example entity",
    attrs=[
        Attr("name", Varchar(), desc="Full name", identifying=True),
        Attr("salary", Decimal(), desc="USD/yr"),
    ],
    fks=emp_rels,
)


dept = Entity(
    name="department",
    desc="collection of employees",
    attrs=[
        Attr("name", Varchar(), identifying=True),
        Attr("total_salary", Decimal()),
    ],
)


################################################################################
################################################################################
################################################################################

objs = [
    dept,
    emp,
]


def make_model(model_name: str) -> Model:
    m = Model(model_name)
    m.add(objs)
    add_generators(m)
    return m
