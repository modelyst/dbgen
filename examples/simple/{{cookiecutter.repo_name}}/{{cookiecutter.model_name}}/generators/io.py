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

from dbgen import Const, Generator, Model
from dbgen.core.node.common_extractors import FileExtractor

from ..constants import DATA_PATH
from ..schema import MyTable
from ..transforms.io import simple_io


def add_io_gens(model: Model):
    with model:
        with Generator("dummy_gen"):
            MyTable.load(insert=True, label=Const("test_label"))

        with Generator("read_file"):
            file_names = FileExtractor(directory=DATA_PATH, extension="txt").results()
            file_contents = simple_io(file_names).results()
            MyTable.load(insert=True, label=file_contents)
