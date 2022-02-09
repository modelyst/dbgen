from alice_bob_model.generators import add_generators

from dbgen import Model


def make_model():
    model = Model(name="alice_bob_lab")
    add_generators(model)
    return model
