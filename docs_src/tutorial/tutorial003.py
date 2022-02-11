from typing import List, Tuple

from tutorials.tutorial2.constants import DEFAULT_ENV

from dbgen import transform


@transform(outputs=["first_name", "last_name", "age"], env=DEFAULT_ENV)
def parse_names(row: List[str]) -> Tuple[str, str, int]:
    first_name = row[0]
    last_name = row[1]
    age = int(row[2])

    return first_name, last_name, age
