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
from io import StringIO
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from dbgen.providers.common.extract import CSVExtractor, FileExtractor, FileNameExtractor, YamlExtractor

# Test data
rows = [
    {'index': '0', 'a': 'i'},
    {'index': '1', 'a': 'j'},
    {'index': '2', 'a': 'k'},
]
columns = [
    'index',
    'a',
]
encoding = 'utf-8'


@pytest.fixture
def example_csv():
    f = StringIO()
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writerows(rows)
    f.seek(0)
    return f


@pytest.fixture
def example_csv_with_header():
    f = StringIO()
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    f.seek(0)
    return f


@pytest.fixture
def test_dir(tmpdir):
    """Test the file extractor by parsing the local directory"""
    directory = tmpdir.mkdir("sub")
    sub_dir = directory.mkdir("sub")
    for dir_id, curr_dir in enumerate((directory, sub_dir)):
        for i in range(10):
            curr_file = curr_dir.join(f"{dir_id}-{i:02d}.txt")
            curr_file.write('')
    assert len(directory.listdir()) == 11
    return directory


file_extractor_ans = [(3, r'[7-9]\.txt'), (1, '01.txt'), (10, None)]


@pytest.mark.parametrize('expected_len,pattern', file_extractor_ans)
def test_file_extractor_old(expected_len, pattern, test_dir):
    extractor = FileExtractor(directory=test_dir, pattern=pattern)
    extractor.setup()
    extract_generator = extractor.extract()
    files = list(extract_generator)
    assert len(files) == expected_len


@pytest.mark.parametrize('expected_len,pattern', file_extractor_ans + [(5, r'1-\d+.txt')])
def test_recursive_file_extractor(expected_len, pattern, test_dir):
    """Test the recursive flag on the file extractor."""
    extractor = FileExtractor(directory=test_dir, pattern=pattern, recursive=True)
    extractor.setup()
    extract_generator = extractor.extract()
    files = list(extract_generator)
    assert len(files) == expected_len * 2


def test_csv_extractor(example_csv_with_header: StringIO):
    reader = csv.DictReader(example_csv_with_header)
    assert next(reader) == {'index': '0', 'a': 'i'}
    assert next(reader) == {'index': '1', 'a': 'j'}
    assert next(reader) == {'index': '2', 'a': 'k'}


def test_csv_with_header(tmpdir: Path, example_csv_with_header: StringIO):
    test_file = tmpdir / 'csv_without_header.csv'
    test_file.write_text(example_csv_with_header.getvalue(), encoding=encoding)
    extract = CSVExtractor(path=test_file, has_header=True, outputs=columns)
    with extract:
        assert extract._reader
        assert extract.length() == 3
        for row, extracted_row in zip(rows, extract.extract()):
            assert row == extracted_row


def test_csv_without_header(tmpdir: Path, example_csv: StringIO):
    test_file = tmpdir / 'csv_with_header.csv'
    test_file.write_text(example_csv.getvalue(), encoding=encoding)
    extract = CSVExtractor(path=test_file, has_header=False, outputs=columns)
    with extract:
        assert extract._reader
        assert extract.length() == 3
        for row, extracted_row in zip(rows, extract.extract()):
            assert row == extracted_row


def test_csv_does_not_exist(tmpdir: Path):
    """Test validation error is thrown when non-existent file is passed in"""
    test_file = tmpdir / 'non_existent.csv'
    with pytest.raises(ValidationError):
        CSVExtractor(path=test_file, outputs=columns)
    CSVExtractor(path=test_file, ensure_path_exists=False, outputs=columns)
    test_file.write_text('', encoding='utf-8')
    CSVExtractor(path=test_file, ensure_path_exists=True, outputs=columns)


def test_file_extractor(tmpdir: Path):
    for i in range(3):
        test_file = tmpdir / f'{i}.txt'
        test_file.write_text(f'{i}', encoding='utf-8')
    extract = FileExtractor(directory=tmpdir, pattern=r'.*\.txt')
    with extract:
        assert extract.length() == 3
        for file_name, contents in extract.extract():
            assert Path(file_name).name.split('.')[0] == contents


def test_file_name_extractor(tmpdir: Path):
    for i in range(3):
        test_file = tmpdir / f'{i}.txt'
        test_file.write_text(f'{i}', encoding='utf-8')
    extract = FileNameExtractor(directory=tmpdir, pattern=r'.*\.txt')
    with extract:
        assert extract.length() == 3
        file_names = [x.name.split('.')[0] for x in sorted(extract.extract())]
        assert file_names == list(map(str, file_names))


def test_file_extractor_no_files(tmpdir: Path):
    """Test the FileExtractor produces 0 rows."""
    extract = FileExtractor(directory=tmpdir)
    with extract:
        assert extract.length() == 0


def test_yaml_extractor(tmpdir: Path):
    """Test the basic use of YamlExtractor."""
    for i in range(3):
        input_dict = {'i': i}
        with open(tmpdir / f'{i}.yml', 'w') as f:
            yaml.dump(input_dict, f)
    extract = YamlExtractor(directory=tmpdir)
    with extract:
        assert extract.length() == 3
        for i, (file_name, yaml_dict) in enumerate(extract.extract()):
            assert yaml_dict['i'] == int(file_name.name.split('.')[0])
