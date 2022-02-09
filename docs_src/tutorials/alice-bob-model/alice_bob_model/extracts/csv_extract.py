import csv
from typing import List

from pydantic import PrivateAttr

from dbgen import Extract


class CSVExtract(Extract):
    data_dir: str
    outputs: List[str] = ["row"]
    _reader: PrivateAttr

    def setup(self, **_):
        csv_file = open(self.data_dir)
        reader = csv.reader(csv_file)
        self._reader = reader

    def extract(self):
        for row in self._reader:
            yield {"row": row}
