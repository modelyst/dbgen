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

from dbgen.core.model import Model
from dbgen.core.model_settings import BaseModelSettings


def test_basic_model_settings():
    model = Model(name='test')
    assert model.settings == BaseModelSettings()


def test_model_setting_inheritance():
    class NewModelSettings(BaseModelSettings):
        setting_1: str = 'val_1'
        setting_2: str = 'val_2'

    model = Model(name='test', settings=NewModelSettings())
    assert model.settings == NewModelSettings()
    assert model.settings.setting_1 == 'val_1'
    assert model.settings.setting_2 == 'val_2'
    model = Model(name='test', settings=NewModelSettings(setting_1='test'))
    assert model.settings.setting_1 == 'test'
