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

from dbgen.core.expr.sqltypes import Boolean, Decimal, Text, Varchar
from dbgen.core.schema import Attr, Entity
from dbgen.core.schema import UserRel as Rel

####
sample = Entity("sample", attrs=[Attr("id", identifying=True)])
####
science = Entity(
    "scientist",
    attrs=[
        Attr("firstname", Varchar()),
        Attr("lastname", Varchar()),
        Attr("ssn", identifying=True, desc="social security"),
    ],
)
####
proced = Entity("procedures", attrs=[Attr("procedure_name", Varchar(), identifying=True)])

####
hist = Entity(
    "history",
    "mapping table",
    attrs=[
        Attr("step", identifying=True, desc="order of operations"),
        Attr("timestamp", Varchar()),
        Attr("step_divides_ssn", Boolean()),
    ],
    fks=[
        Rel("sample", identifying=True),
        Rel("expt_type", "procedures", identifying=True),
        Rel(
            "operator",
            "scientist",
        ),
    ],
)

####
hd = Entity(
    "history_detail",
    "RDF triplestore",
    attrs=[
        Attr("name", Varchar(), identifying=True),
        Attr("value", Text()),
        Attr("dtype", Varchar()),
    ],
    fks=[Rel("history", identifying=True)],
)
#####
elec = Entity(
    "electrode",
    "Either an anode or cathode",
    attrs=[Attr("composition", Varchar())],
    fks=[Rel("sample", identifying=True)],
)
#####

anode = Entity("anode", fks=[Rel("electrode", identifying=True)])
cathode = Entity("cathode", fks=[Rel("electrode", identifying=True)])

#####
fc = Entity(
    "fuel_cell",
    "Combination of a particular anode and cathode sample during an expt",
    attrs=[
        Attr("expt_id", identifying=True),
        Attr("electrolyte", Varchar()),
        Attr("capacity", Decimal()),
        Attr("timestamp", Varchar()),
        Attr("calc_anode", Boolean()),
    ],
    fks=[Rel("anode"), Rel("cathode")],
)
#####
ec = Entity(
    "electrode_composition",
    "mapping table",
    attrs=[Attr("frac", Decimal())],
    fks=[Rel("electrode"), Rel("element")],
)
#####
elem = Entity(
    "element",
    "Atomic elements",
    attrs=[
        Attr("atomic_number", identifying=True),
        Attr("symbol", Varchar()),
        Attr("name", Varchar()),
        Attr("mass", Decimal()),
    ],
)

all = [
    sample,
    science,
    proced,
    hist,
    hd,
    elec,
    anode,
    cathode,
    fc,
    elem,
    ec,
]
