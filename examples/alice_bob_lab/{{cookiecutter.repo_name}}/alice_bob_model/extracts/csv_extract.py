import csv
from typing import List

from dbgen import Extract


class CSVExtract(Extract):
    data_dir: str
    outputs: List[str] = ["row"]

    def extract(self):
        with open(self.data_dir) as csv_file:
            reader = csv.reader(csv_file)
            yield from reader
