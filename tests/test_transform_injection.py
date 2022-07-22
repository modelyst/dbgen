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


from dbgen import transform
from dbgen.core.decorators import extract
from dbgen.core.etl_step import ETLStep
from dbgen.core.model_settings import BaseModelSettings
from dbgen.testing.runner import ETLStepTestRunner


class NewModelSettings(BaseModelSettings):
    setting_value: str = 'default'


def test_setting_injection():
    @extract
    def extractor():
        yield 1

    @transform
    def first_func(extracted_val, settings):
        assert settings.setting_value == 'default'
        return extracted_val

    with ETLStep(name='test_step') as etl_step:
        out = extractor().results()
        first_func(out)

    run = ETLStepTestRunner().test(etl_step, settings=NewModelSettings())
    assert run.status == 'completed'
    run = ETLStepTestRunner().test(etl_step, settings=NewModelSettings(setting_value='test'))
    assert run.status == 'failed'
