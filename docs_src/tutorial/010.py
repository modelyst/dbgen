from dbgen import Entity, Extract, Model, Generator, Env, Import, transform

import numpy
import os


class JVCurve(Entity, table=True):
    full_path: str
    short_circuit_current_density: float
    open_circuit_voltage: float
    __identifying__ = {"full_path"}


class LocalCSVExtract(Extract):
    data_dir: str

    def setup(self):
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


def make_model():

    with Model(name="example") as model:
        with Generator('load_jv_curves'):
            extract = LocalCSVExtract(data_dir=os.environ['DATA_DIR'], outputs=['file_path'])
            file_path = extract['file_path']

            @transform(env=Env([Import('numpy')]), outputs=['voc', 'jsc'])
            def parse_jv_csv(file_path: str) -> tuple[float, float]:
                jv_arr = numpy.genfromtxt(file_path, delimiter=',', skip_header=1, dtype=float)
                for row in range(numpy.shape[0]):
                    if jv_arr[row][0] > 0:
                        jsc = jv_arr[row][1]
                        break

                for row in range(numpy.shape[0]):
                    if jv_arr[row][1] > 0:
                        voc = jv_arr[row][0]
                        break

                return voc, jsc

            voc, jsc = parse_jv_csv(file_path).results()

            JVCurve.load(
                insert=True, full_path=file_path, open_circuit_voltage=voc, short_circuit_current_density=jsc
            )

    return model
