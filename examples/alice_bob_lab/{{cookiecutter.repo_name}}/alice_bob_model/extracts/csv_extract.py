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
