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

import re
from pathlib import Path
from typing import Generator as GenType
from typing import List, Optional

from pydantic import DirectoryPath

from dbgen.core.node.extract import Extract


class FileExtractor(Extract[Path]):
    directory: DirectoryPath
    pattern: Optional[str]
    recursive: bool = False
    outputs: List[str] = ['file_name']
    _file_paths: List[Path]

    def setup(self, **_):
        file_gen = self.directory.rglob('*') if self.recursive else self.directory.iterdir()
        self._file_paths = [
            x for x in file_gen if x.is_file() and (self.pattern is None or re.search(self.pattern, x.name))
        ]

    def extract(self) -> GenType[Path, None, None]:
        yield from self._file_paths

    def length(self, **_):
        return len(self._file_paths)
