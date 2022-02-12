from os.path import join
from typing import List, Tuple

from tutorials.tutorial2.constants import DATA_DIR, DEFAULT_ENV
from tutorials.tutorial2.extracts.csv_extract import CSVExtract
from tutorials.tutorial2.schema import Person

from dbgen import ETLStep, Model, transform


@transform(outputs=["first_name", "last_name", "age"], env=DEFAULT_ENV)
def parse_names(row: List[str]) -> Tuple[str, str, int]:
    first_name = row[0]
    last_name = row[1]
    age = int(row[2])

    return first_name, last_name, age


def add_io_etl_step(model: Model) -> None:
    with model:
        with ETLStep(name="names"):
            row = CSVExtract(data_dir=join(DATA_DIR, "names.csv")).results()
            first_name, last_name, age = parse_names(row).results()
            Person.load(insert=True, first_name=first_name, last_name=last_name, age=age)
