from tutorials.alice_bob_lab.generators import add_generators

from dbgen import Model


def make_model():
    model = Model(name="alice_bob_lab")
    add_generators(model)
    return model
