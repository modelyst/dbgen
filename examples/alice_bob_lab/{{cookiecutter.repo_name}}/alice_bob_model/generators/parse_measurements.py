import re
from os.path import join
from typing import Tuple

from alice_bob_model.constants import DATA_DIR, DEFAULT_ENV
from alice_bob_model.extracts.measurement_extract import MeasurementExtract
from alice_bob_model.schema import Person, TemperatureMeasurement

from dbgen import Environment, Generator, Import, Model, transform


@transform(
    outputs=["first_name", "last_name", "ordering", "temperature"],
    env=DEFAULT_ENV + Environment(Import("re")),
)
def parse_measurements(file_name: str, contents: str) -> Tuple[str, str, int, float]:
    regex = r"([A-Za-z]+)_([A-Za-z]+)_(\d+).txt"
    match = re.match(regex, file_name)
    if not match:
        raise ValueError("No match found")
    first_name, last_name, ordering_str = match.groups()
    ordering = int(ordering_str)

    regex = r".*:\s*(\d+)"
    match = re.match(regex, contents)
    if not match:
        raise ValueError("No match found")
    (temperature_str,) = match.groups()
    temperature = float(temperature_str)

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
