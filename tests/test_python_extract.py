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

from typing import Tuple

from dbgen import BaseModelSettings, ETLStep
from dbgen.core.decorators import extract, transform
from dbgen.testing.runner import ETLStepTestRunner


@extract(outputs=['n', 'str_n'])
def simple_extract(n: int = 10) -> Tuple[int, str]:
    for i in range(n):
        yield i, str(i)


@transform
def simple_transform(n: int):
    return n + 1


def test_basic_python_extract_initialization():
    with ETLStep(name='test') as step:
        simple_extract().results()

    test_run = ETLStepTestRunner().test(step)
    assert test_run.number_of_extracted_rows == 10
    assert test_run.status == 'completed'


def test_basic_python_extract_initialization_with_arg():
    with ETLStep(name='test') as step:
        extractor = simple_extract(100)
        n, str_n = extractor.results()
        simple_transform(extractor['n']).results()

    assert len(step.transforms) == 1
    test_run = ETLStepTestRunner().test(step)
    assert test_run.number_of_extracted_rows == 100
    assert test_run.status == 'completed'


class ModelSettings(BaseModelSettings):
    n_value: int = 10


@extract
def setting_extract(settings: ModelSettings) -> Tuple[int, str]:
    for i in range(settings.n_value):
        yield i, str(i)


def test_basic_python_extract_initialization_with_settings():
    with ETLStep(name='test') as step:
        setting_extract()

    test_run = ETLStepTestRunner().test(step, settings=ModelSettings(n_value=100))
    assert test_run.number_of_extracted_rows == 100
    assert test_run.status == 'completed'
