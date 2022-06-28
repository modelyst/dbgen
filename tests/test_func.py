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

from dbgen.core.func import Func, func_from_callable, get_callable_source_code


def basic_function(arg_1: int, arg_2: str, arg_3: float) -> str:
    return f"{arg_1}->{arg_2}->{arg_3}"


def no_arg_func() -> int:
    return 1


basic_lambda = lambda arg_1, arg_2, arg_3: f"{arg_1}->{arg_2}->{arg_3}"


def test_function_parser(tmpdir):
    assert basic_function(1, 1, 1) == "1->1->1"
    source_code = get_callable_source_code(basic_function)
    assert isinstance(source_code, str)
    p = tmpdir.mkdir("sub").join("test.py")
    p.write(source_code)
    func = Func.path_to_func(p)
    assert func(1, 1, 1) == basic_function(1, 1, 1)


def test_lambda_parser(tmpdir):
    assert basic_lambda(1, 1, 1) == "1->1->1"
    source_code = get_callable_source_code(basic_function)
    assert isinstance(source_code, str)
    p = tmpdir.mkdir("sub").join("test.py")
    p.write(source_code)
    func = Func.path_to_func(p)
    assert func(1, 1, 1) == basic_function(1, 1, 1)


def test_func_from_callable():
    func = func_from_callable(basic_function)
    assert func.name == "basic_function"
    assert func == Func.parse_obj(func.dict())


def test_func_contains_original_function():
    """Assert that standard func will store original function at runtime."""
    func = func_from_callable(no_arg_func)
    assert func._func == no_arg_func


def test_func_points_to_temp_function():
    func = func_from_callable(no_arg_func)
    func.store_func(force=True)
    assert func._func != no_arg_func


def test_func_path_name():
    func = func_from_callable(no_arg_func)
    func.store_func(force=True)
    assert func._func != no_arg_func
