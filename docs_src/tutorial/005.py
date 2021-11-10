import os

from dbgen import Extract


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
