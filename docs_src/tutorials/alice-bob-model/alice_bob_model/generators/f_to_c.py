from scipy.constants import convert_temperature
from sqlmodel import select

from dbgen import Env, Generator, Import, Model, Query, transform

from ..constants import DEFAULT_ENV
from ..schema import TemperatureMeasurement

outputs = ["temp_c"]
env = DEFAULT_ENV + Env([Import("scipy.constants", "convert_temperature")])


@transform(outputs=outputs, env=env)
def f_to_c(temp_f: float) -> float:
    temp_c = convert_temperature(temp_f, "F", "C")

    return temp_c


def add_f_to_c(model: Model) -> None:
    with model:
        with Generator(name="f_to_c"):
            temperature_measurement_id, temp_f = Query(
                select(TemperatureMeasurement.id, TemperatureMeasurement.temperature_F)
            ).results()
            temp_c = f_to_c(temp_f).results()
            TemperatureMeasurement.load(
                id=temperature_measurement_id,
                temperature_C=temp_c,
            )
