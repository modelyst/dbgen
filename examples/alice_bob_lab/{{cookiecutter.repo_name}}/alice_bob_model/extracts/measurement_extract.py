from os import listdir
from os.path import join
from typing import List

from dbgen import Extract


class MeasurementExtract(Extract):
    data_dir: str
    outputs: List[str] = ["filename", "contents"]

    def extract(self):
        fnames = listdir(self.data_dir)
        for fname in fnames:
            with open(join(self.data_dir, fname)) as f:
                contents = f.read()
            yield fname, contents
