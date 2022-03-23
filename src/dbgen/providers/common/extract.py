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
import re
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, TextIO, Tuple, TypeVar

import yaml
from pydantic import DirectoryPath, root_validator

from dbgen.core.node.extract import Extract

T = TypeVar('T')
T2 = TypeVar('T2', bound=str)


class _FileExtractorBase(Extract[T]):
    directory: DirectoryPath
    pattern: Optional[str]
    recursive: bool = False
    _file_paths: List[Path]

    def setup(self, **_):
        file_gen = self.directory.rglob('*') if self.recursive else self.directory.iterdir()
        self._file_paths = [
            x for x in file_gen if x.is_file() and (self.pattern is None or re.search(self.pattern, x.name))
        ]

    def length(self, **_):
        return len(self._file_paths)


class FileNameExtractor(_FileExtractorBase[Path]):
    outputs: List[str] = ['file_name']

    def extract(self) -> Generator[Path, None, None]:
        yield from self._file_paths


class FileExtractor(_FileExtractorBase[Tuple[Path, Any]]):
    outputs: List[str] = ['file_name', 'parsed_file']

    def file_parser(self, file_obj: TextIO) -> str:
        return file_obj.read()

    def extract(self) -> Generator[Tuple[Path, Any], None, None]:
        for file_name in self._file_paths:
            with open(file_name) as f:
                yield file_name, self.file_parser(f)


class YamlExtractor(FileExtractor):
    pattern: Optional[str] = r'.*\.(yaml|yml)'

    def file_parser(self, file_obj: TextIO):
        return yaml.safe_load(file_obj)


class CSVExtractor(Extract[Dict[str, str]]):
    """Extract each line in a CSV into its own input row."""

    path: Path
    ensure_path_exists: bool = True
    has_header: bool = True
    delimiter: str = ','
    outputs: List[str]
    _file: TextIOWrapper
    _reader: csv.DictReader

    @root_validator
    def validate_path_exists(cls, values):
        if not values['path'].exists() and values['ensure_path_exists']:
            raise ValueError(f"CSV {values['path']} does not exist.")
        return values

    def setup(self):
        self._file = self.path.open('r')
        fieldnames = None if self.has_header else self.outputs
        self._reader = csv.DictReader(self._file, fieldnames=fieldnames)

    def teardown(self):
        self._file.close()

    def length(self):
        count = sum(1 for _ in self._reader)
        self._file.seek(0)
        fieldnames = None if self.has_header else self.outputs
        self._reader = csv.DictReader(self._file, fieldnames=fieldnames)
        return count

    def extract(self) -> Generator[Dict[str, str], None, None]:
        yield from self._reader
