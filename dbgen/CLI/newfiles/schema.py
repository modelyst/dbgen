# Internal Modules
from dbgen import Model, Obj, Attr, Rel, Varchar, Decimal
from .generators import add_generators  # type: ignore

emp_rels = [Rel("department"), Rel("manager", "employee")]
emp = Obj(
    name="employee",
    desc="an example entity",
    attrs=[
        Attr("name", Varchar(), desc="Full name", identifying=True),
        Attr("salary", Decimal(), desc="USD/yr"),
    ],
    fks=emp_rels,
)


dept = Obj(
    name="department",
    desc="collection of employees",
    attrs=[Attr("name", Varchar(), identifying=True), Attr("total_cost", Decimal())],
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
