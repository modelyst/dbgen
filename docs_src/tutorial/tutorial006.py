import numpy as np

from dbgen import Env, Import
from dbgen.core.decorators import transform


@transform(env=Env(Import('numpy', lib_alias='np')), outputs=['voc', 'jsc'])
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
