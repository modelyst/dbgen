#   Copyright 2022 Modelyst LLC
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

import os
from typing import List

import numpy as np

from dbgen import Entity, Environment, ETLStep, Extract, Import, Model
from dbgen.core.decorators import transform


class JVCurve(Entity, table=True):
    full_path: str
    short_circuit_current_density: float
    open_circuit_voltage: float
    __identifying__ = {"full_path"}


class LocalCSVExtract(Extract):
    data_dir: str
    outputs: List[str] = ['file_path']
    _file_paths = None

    def setup(self, **_):
        self._file_paths = [
            os.path.join(self.data_dir, fname)
            for fname in os.listdir(self.data_dir)
            if fname.endswith('.csv')
        ]

    def extract(self):
        for file_path in self._file_paths:
            output_dict = {'file_path': file_path}
            yield output_dict

    def length(self, **_):
        return len(self._file_paths)


@transform(env=Environment(Import('numpy', lib_alias='np')), outputs=['voc', 'jsc'])
def parse_jv_csv(file_path: str) -> tuple[float, float]:
    jv_arr = np.genfromtxt(file_path, delimiter=',', skip_header=1, dtype=float)
    nrows, *_ = np.shape(jv_arr)
    for row in range(nrows):
        if jv_arr[row][0] > 0:
            jsc = jv_arr[row][1]
            break

    for row in range(nrows):
        if jv_arr[row][1] > 0:
            voc = jv_arr[row][0]
            break

    return voc, jsc


def make_model():
    with Model(name="example") as model:
        with ETLStep('load_jv_curves'):
            extract = LocalCSVExtract(data_dir=os.environ['DATA_DIR'])
            file_path = extract['file_path']
            voc, jsc = parse_jv_csv(file_path).results()

            JVCurve.load(
                insert=True, full_path=file_path, open_circuit_voltage=voc, short_circuit_current_density=jsc
            )

    return model
