from os.path import join
from typing import List, Tuple

from dbgen import Generator, Model, transform
from tutorials.tutorial2.constants import DATA_DIR, DEFAULT_ENV
from tutorials.tutorial2.extracts.csv_extract import CSVExtract
from tutorials.tutorial2.schema import Person


@transform(outputs=["first_name", "last_name", "age"], env=DEFAULT_ENV)
def parse_names(row: List[str]) -> Tuple[str, str, int]:
    first_name = row[0]
    last_name = row[1]
    age = int(row[2])

    return first_name, last_name, age


