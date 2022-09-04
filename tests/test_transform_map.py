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

from dbgen import Constant, ETLStep
from dbgen.core.decorators import mappable_transform
from dbgen.core.node.transforms import PythonTransform
from dbgen.testing import ETLStepTestRunner


def transform_function(x: int):
    return x % 2


def test_basic_map():
    input_val = Constant(list(range(100)))
    python_transform = PythonTransform(inputs=[input_val], function=transform_function)
    python_transform.run({})


@mappable_transform
def func(x, y, z: int = 0):
    return x + y + z


def test_map_etl_step():
    with ETLStep(name='test') as test:

        added = func.map(Constant(list(range(10))), Constant(list(range(10))), z=Constant([2, 4])).results()
        func.map(added, added, z=3)

    ETLStepTestRunner(log_level='DEBUG').test(test)
    assert False
