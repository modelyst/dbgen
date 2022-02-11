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

from os.path import join
from typing import Tuple

from alice_bob_model.constants import DATA_DIR, DEFAULT_ENV
from alice_bob_model.extracts.measurement_extract import MeasurementExtract
from alice_bob_model.schema import Person, TemperatureMeasurement

from dbgen import Generator, Model, transform

outputs = ["first_name", "last_name", "ordering", "temperature"]
env = DEFAULT_ENV


@transform(outputs=outputs, env=env)
def parse_measurements(file_name: str, contents: str) -> Tuple[str, str, float]:
    file_name = file_name.strip(".txt")
    first_name, last_name, ordering_str = file_name.split("_")
    ordering = int(ordering_str)
    temperature = float(contents.split(": ")[-1])

    return first_name, last_name, ordering, temperature


def add_temperature_generator(model: Model) -> None:
    with model:
        with Generator(name="temperature"):
            filename, contents = MeasurementExtract(data_dir=join(DATA_DIR, "measurements")).results()
            first_name, last_name, ordering, temperature = parse_measurements(filename, contents).results()
            TemperatureMeasurement.load(
                insert=True,
                temperature_F=temperature,
                ordering=ordering,
                person_id=Person.load(first_name=first_name, last_name=last_name),
            )
