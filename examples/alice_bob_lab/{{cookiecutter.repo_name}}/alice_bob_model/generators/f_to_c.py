#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from alice_bob_model.constants import DEFAULT_ENV
from alice_bob_model.schema import TemperatureMeasurement
from scipy.constants import convert_temperature
from sqlmodel import select

from dbgen import Env, Generator, Import, Model, Query, transform

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
