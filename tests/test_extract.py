#   Copyright 2022 Modelyst LLC
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

from typing import List, Optional, Tuple

import pytest

from dbgen.core.node.extract import Extract


def run_extract(extract: Extract) -> Tuple[List[dict], Optional[int]]:
    outputs = []
    with extract:
        extract_generator = extract.extract()
        length = extract.length()
        for extracted_val in extract_generator:
            outputs.append({extract.hash: extract.process_row(extracted_val)})
    return outputs, length


def test_basic_extract():
    """Test the Base Extract."""
    extract = Extract()
    outputs, length = run_extract(extract)
    assert length is None
    assert len(outputs) == 1
    assert outputs[0] == {extract.hash: {}}


class BaseCustomExtract(Extract[str]):
    outputs: List[str] = ['char']
    string: str

    def length(self):
        return len(self.string)

    def extract(self):
        yield from self.string


class CustomExtract(BaseCustomExtract):
    _status: str = 'initialized'

    def setup(self):
        self._status = 'setup'

    def teardown(self):
        self._status = 'finished'

    def extract(self):
        assert self._status == 'setup'
        return super().extract()


class ErrorCustomExtract(CustomExtract):
    def extract(self):
        raise ValueError('Error')


def test_basic_custom_extract():
    """Test that a custom extract has the correct length and extract methods."""

    string = 'abcd'
    extract = BaseCustomExtract(string='abcd')
    outputs, length = run_extract(extract)
    assert length == len(string)
    assert len(outputs) == len(string)
    assert outputs == [{extract.hash: {'char': char}} for char in string]


def test_custom_setup_teardown():
    """Test that a custom extract's setup and teardown methods are correctly called in order."""

    string = 'abcd'
    extract = CustomExtract(string='abcd')
    assert extract._status == 'initialized'
    outputs, length = run_extract(extract)
    assert length == len(string)
    assert len(outputs) == len(string)
    assert outputs == [{extract.hash: {'char': char}} for char in string]
    assert extract._status == 'finished'


def test_teardown_called_during_exception():
    extract = ErrorCustomExtract(string='abcd')
    assert extract._status == 'initialized'
    with pytest.raises(ValueError):
        run_extract(extract)
    assert extract._status == 'finished'


class BadTupleExtract(Extract[tuple]):
    outputs: List[str] = ['tuple']

    def extract(self):
        yield (1, 2, 3)


class GoodTupleExtract(Extract[tuple]):
    outputs: List[str] = ['tuple']

    def extract(self):
        yield ((1, 2, 3),)


class ListExtract(Extract[list]):
    outputs: List[str] = ['list']

    def extract(self):
        yield [1, 2, 3]


def test_bad_tuple_return():
    """Test that error is raised when a N-length tuple is returned for a 1-length output extract"""
    with pytest.raises(ValueError, match='Expected 1 output from extract '):
        extract = BadTupleExtract()
        run_extract(extract)


def test_good_return():
    """Test that lists and tuples can be returned as 1 output"""
    extract = GoodTupleExtract()
    outputs, _ = run_extract(extract)
    assert outputs == [{extract.hash: {'tuple': (1, 2, 3)}}]

    extract = ListExtract()
    outputs, _ = run_extract(extract)
    assert outputs == [{extract.hash: {'list': [1, 2, 3]}}]
