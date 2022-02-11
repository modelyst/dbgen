from alice_bob_model.etl_steps.f_to_c import add_f_to_c
from alice_bob_model.etl_steps.parse_measurements import add_temperature_etl_step
from alice_bob_model.etl_steps.read_csv import add_io_etl_step

from dbgen import Model


def add_etl_steps(model: Model) -> None:
    add_io_etl_step(model)
    add_temperature_etl_step(model)
    add_f_to_c(model)
