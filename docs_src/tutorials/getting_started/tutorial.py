from dbgen import Constant, Entity, Generator, Model


class Person(Entity, table=True, all_identifying=True):
    first_name: str


def make_model() -> Model:
    model = Model(name="alice_bob_lab")
    with model:
        with Generator(name="insert_name"):
            Person.load(insert=True, first_name=Constant(["Alice"]))

    return model
