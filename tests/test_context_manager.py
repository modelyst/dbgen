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

from typing import Any, Dict, Tuple, TypedDict

from dbgen import Const, Env, Import, PyBlock
from dbgen.core.wrappers import node

# def add_one_simple(val: int) -> int:
#     return val + 1


# @node
# def add_one(val: int, t: str = 'asdf') -> tuple[int, str, float]:
#     return val + 1, t + 'test', 1.0


# class Response(TypedDict):
#     message: str
#     code: int


# @node
# def get_response(url: str) -> Dict[str, Any]:
#     return {'message': 'hello world', "code": 42}


# @node()
# def test_with_env(val: int, t: str = 'asdf') -> tuple[int, str]:
#     return val + 1, t + 'test'


# test_pb = PyBlock(function=add_one_simple, inputs=[Const(1)], outputs=['a', 'b'])

# pb = add_one(inputs=[test_pb['a']], outputs=['a', 'b'])
# a = pb[0]
# x = iter(pb)

# print(t(1, 'asdf'))
# print(test_with_env(1, 'asdf'))


@node
def add_one(x: int = 0):
    return x + 1


@node(env=Env([Import('numpy')]))
def subtract_one(x: int) -> int:
    return x - 1


task = add_one(1)
out = task.results()
task_2 = add_one(out)
task_3 = subtract_one(1)
# Entity.load(val=task_3[0])


# pb = add_one(Const(1))
# pb_2 = add_one(pb.results())
# pb_3 = subtract_one(pb_2.results())


def complex_func() -> Tuple[int, str, dict]:
    return 1, 'a', {}


complex_node = node(complex_func)


a, b, c = complex_node().results()
task = complex_node()
a, b, c = task
