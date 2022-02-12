from alice_bob_model.constants import DEFAULT_ENV
from alice_bob_model.schema import TemperatureMeasurement
from sqlmodel import select

from dbgen import ETLStep, Model, Query, transform


@transform(outputs=["temp_c"], env=DEFAULT_ENV)
def f_to_c(temp_f: float) -> float:
    temp_c = (temp_f - 32.0) * 5.0 / 9.0

    return temp_c


def add_f_to_c(model: Model) -> None:
    with model:
        with ETLStep(name="f_to_c"):
            temperature_measurement_id, temp_f = Query(
                select(TemperatureMeasurement.id, TemperatureMeasurement.temperature_F)
            ).results()
            temp_c = f_to_c(temp_f).results()
            TemperatureMeasurement.load(
                id=temperature_measurement_id,
                temperature_C=temp_c,
            )
