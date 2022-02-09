from os import listdir
from os.path import join
from typing import List

from pydantic import PrivateAttr

from dbgen import Extract


class MeasurementExtract(Extract):
    data_dir: str
    outputs: List[str] = ["filename", "contents"]
    _fnames: PrivateAttr

    def setup(self, **_):
        self._fnames = listdir(self.data_dir)

    def extract(self):
        for fname in self._fnames:
            f = open(join(self.data_dir, fname))
            contents = f.read()
            f.close()
            yield {"filename": fname, "contents": contents}
