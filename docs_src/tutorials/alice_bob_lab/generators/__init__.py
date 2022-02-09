from tutorials.alice_bob_lab.generators.f_to_c import add_f_to_c
from tutorials.alice_bob_lab.generators.parse_measurements import add_temperature_generator
from tutorials.alice_bob_lab.generators.read_csv import add_io_generator

from dbgen import Model


def add_generators(model: Model) -> None:
    add_io_generator(model)
    add_temperature_generator(model)
    add_f_to_c(model)
