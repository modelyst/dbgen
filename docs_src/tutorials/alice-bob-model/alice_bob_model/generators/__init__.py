from dbgen import Model

from .f_to_c import add_f_to_c
from .parse_measurements import add_temperature_generator
from .read_csv import add_io_generator


def add_generators(model: Model) -> None:
    add_io_generator(model)
    add_temperature_generator(model)
    add_f_to_c(model)
