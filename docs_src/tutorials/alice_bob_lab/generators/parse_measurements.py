from os.path import join
from typing import Tuple

from dbgen import Generator, Model, transform
from tutorials.alice_bob_lab.constants import DATA_DIR, DEFAULT_ENV
from tutorials.alice_bob_lab.extracts.measurement_extract import MeasurementExtract
from tutorials.alice_bob_lab.schema import Person, TemperatureMeasurement

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
