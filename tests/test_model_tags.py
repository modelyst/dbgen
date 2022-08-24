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

from dbgen.core.etl_step import ETLStep
from dbgen.core.model import Model
from dbgen.core.tags import tags


def test_model_tags():
    with Model(name='test'):
        with tags('a'):
            with tags('b'):
                with ETLStep(name='step_b', tags=['step_b']) as step_b:
                    pass
            with tags('c'):
                with ETLStep(name='step_c') as step_c:
                    pass
            with ETLStep(name='step_d') as step_d:
                pass
        with ETLStep(name='step_e') as step_e:
            pass

    assert step_b.tags == ['a', 'b', 'step_b']
    assert step_c.tags == ['a', 'c']
    assert step_d.tags == ['a']
    assert step_e.tags == []
