from dbgen.core.decorators import node
from dbgen import Env, Import
import numpy as np


@node(env=Env(Import(['numpy', 'np'])), outputs=['voc', 'jsc'])
def parse_jv_csv(file_path: str) -> tuple[float, float]:
    jv_arr = np.genfromtxt(file_path, delimiter=',', skip_header=1, dtype=float)
    for row in range(np.shape[0]):
        if jv_arr[row][0] > 0:
            jsc = jv_arr[row][1]
            break

    for row in range(np.shape[0]):
        if jv_arr[row][1] > 0:
            voc = jv_arr[row][0]
            break

    return voc, jsc
