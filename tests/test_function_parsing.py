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

"""Tests related to testing function parsing."""
from dbgen import Environment, Import, transform
from dbgen.core.func import get_callable_source_code


def simple_test():
    pass


def test_simple_function():
    source_code = get_callable_source_code(simple_test)
    assert source_code == """def simple_test():\n    pass"""


@transform(env=Environment([Import('typing'), Import('math')]))
def test():
    pass


def test_decorated_function():
    source_code = get_callable_source_code(test().function)
    assert source_code == """def test():\n    pass"""


@transform(
    env=Environment(
        [
            Import('math'),
            Import('math'),
            Import('math'),
            Import('math'),
            Import('math'),
            Import('math'),
            Import('math'),
        ]
    ),
)
def multi_line_test():
    pass


def test_multi_line_decorator():
    source_code = get_callable_source_code(multi_line_test().function)
    assert source_code == """def multi_line_test():\n    pass"""


def test_nested_function():
    @transform(env=Environment([Import('typing'), Import('math')]))
    def nested_test():
        pass

    source_code = get_callable_source_code(nested_test().function)
    assert source_code == """def nested_test():\n    pass"""
