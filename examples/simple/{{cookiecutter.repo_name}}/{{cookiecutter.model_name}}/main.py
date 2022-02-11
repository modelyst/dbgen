from dbgen import Model

from . import schema  # noqa: F401
from .generators import add_generators


def make_model():
    model = Model(name="{{cookiecutter.model_name}}")
    # add gens to model
    add_generators(model)
    return model
