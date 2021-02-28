# Internal Modules
from dbgen import Model

from .io import io
from .analysis import analysis

#############################################################


def add_generators(mod: Model) -> None:
    io(mod)
    analysis(mod)
