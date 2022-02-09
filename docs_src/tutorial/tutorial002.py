import csv
from typing import List

from pydantic import PrivateAttr

from dbgen import Extract


class CSVExtract(Extract):
    outputs: List[str] = ["row"]
    data_dir: str
    _reader: PrivateAttr

    def setup(self, **_):
        csv_file = open(self.data_dir)
        reader = csv.reader(csv_file)
        self._reader = reader

    def extract(self):
        for row in self._reader:
            yield {"row": row}
