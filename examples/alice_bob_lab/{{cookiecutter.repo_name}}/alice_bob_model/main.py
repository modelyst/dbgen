from alice_bob_model.etl_steps import add_etl_steps

from dbgen import Model


def make_model():
    model = Model(name="alice_bob_lab")
    add_etl_steps(model)
    return model
