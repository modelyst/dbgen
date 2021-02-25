from dbgen.core.schema import Obj, Attr, UserRel as Rel
from dbgen.core.expr.sqltypes import Varchar, Decimal, Text, Boolean


####
sample = Obj("sample", attrs=[Attr("id", identifying=True)])
####
science = Obj(
    "scientist",
    attrs=[
        Attr("firstname", Varchar()),
        Attr("lastname", Varchar()),
        Attr("ssn", identifying=True, desc="social security"),
    ],
)
####
proced = Obj("procedures", attrs=[Attr("procedure_name", Varchar(), identifying=True)])

####
hist = Obj(
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
        Rel("operator", "scientist",),
    ],
)

####
hd = Obj(
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
elec = Obj(
    "electrode",
    "Either an anode or cathode",
    attrs=[Attr("composition", Varchar())],
    fks=[Rel("sample", identifying=True)],
)
#####

anode = Obj("anode", fks=[Rel("electrode", identifying=True)])
cathode = Obj("cathode", fks=[Rel("electrode", identifying=True)])

#####
fc = Obj(
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
ec = Obj(
    "electrode_composition",
    "mapping table",
    attrs=[Attr("frac", Decimal())],
    fks=[Rel("electrode"), Rel("element")],
)
#####
elem = Obj(
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
