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

import pytest

from dbgen.core.node.common_extractors import FileExtractor


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
def test_file_extractor(expected_len, pattern, test_dir):
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
