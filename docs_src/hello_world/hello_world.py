from dbgen import Constant, Entity, ETLStep, Model


class SimpleTable(Entity, table=True):
    __identifying__ = {"string_column"}
    __tablename__ = "simple_table"
    string_column: str


def make_model() -> Model:
    model = Model(name='hello_world')
    with model:
        with ETLStep(name="simplest_possible"):
            SimpleTable.load(insert=True, string_column=Constant('hello world'))

    return model
