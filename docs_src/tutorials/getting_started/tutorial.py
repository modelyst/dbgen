from dbgen import Model, Generator, Entity, Const


class Person(Entity, table=True, all_identifying=True):
    first_name: str


def make_model() -> Model:
    model = Model(name="alice_bob_lab")
    with model:
        with Generator(name="insert_name"):
            Person.load(insert=True, first_name=Const(["Alice"]))

    return model
