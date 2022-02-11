from dbgen import Model

from . import schema  # noqa: F401
from .etl_steps import add_etl_steps


def make_model():
    model = Model(name="{{cookiecutter.model_name}}")
    # add etl_steps to model
    add_etl_steps(model)
    return model
