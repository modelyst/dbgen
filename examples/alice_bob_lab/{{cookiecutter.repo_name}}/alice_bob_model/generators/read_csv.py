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
from typing import List, Tuple

from alice_bob_model.constants import DATA_DIR, DEFAULT_ENV
from alice_bob_model.extracts.csv_extract import CSVExtract
from alice_bob_model.schema import Person

from dbgen import Generator, Model, transform


@transform(outputs=["first_name", "last_name", "age"], env=DEFAULT_ENV)
def parse_names(row: List[str]) -> Tuple[str, str, int]:
    first_name = row[0]
    last_name = row[1]
    age = int(row[2])

    return first_name, last_name, age


def add_io_generator(model: Model) -> None:
    with model:
        with Generator(name="names"):
            row = CSVExtract(data_dir=join(DATA_DIR, "names.csv")).results()  # extract
            first_name, last_name, age = parse_names(row).results()  # transform
            Person.load(insert=True, first_name=first_name, last_name=last_name, age=age)  # load
