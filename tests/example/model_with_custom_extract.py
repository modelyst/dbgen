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

from typing import List

from pydantic.fields import PrivateAttr

from dbgen import Entity, Generator, Model
from dbgen.core.node.extract import Extract

dummy_file_system = {
    'dir_1': {f'dir_1_file_{i}.txt': str(i) for i in range(10)},
    'dir_2': {f'dir_2:file_{i}.txt': str(i) for i in range(int(10e5))},
}


class CustomExtractor(Extract):
    directory: str
    _file_list: List[str] = PrivateAttr(None)
    outputs: List[str] = ['file_name']

    def setup(self, *, connection=None, yield_per=None, **kwargs):
        self._file_list = dummy_file_system[self.directory]

    def extract(self):
        for file_name in self._file_list:
            yield {'file_name': file_name}

    def length(self, *, connection=None):
        return len(self._file_list)


model = Model(name='model_with_custom_extractor')


class File(Entity, table=True):
    __identifying__ = {'file_name'}
    file_name: str


extract = CustomExtractor(directory='dir_2')
load = File.load(insert=True, file_name=extract['file_name'])
gen = Generator(name='load_files', extract=extract, loads=[load])
model.add_gen(gen)
