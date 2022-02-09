from dbgen import Const, Entity, Generator, Model


class SimpleTable(Entity, table=True, all_identifying=True):
    string_column: str


def make_model() -> Model:
    model = Model(name='hello_world')
    with model:
        with Generator(name="simplest_possible"):
            SimpleTable.load(insert=True, string_column=Const('hello world'))

    return model
