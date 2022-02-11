from alice_bob_model.generators.f_to_c import add_f_to_c
from alice_bob_model.generators.parse_measurements import add_temperature_generator
from alice_bob_model.generators.read_csv import add_io_generator

from dbgen import Model


def add_generators(model: Model) -> None:
    add_io_generator(model)
    add_temperature_generator(model)
    add_f_to_c(model)
