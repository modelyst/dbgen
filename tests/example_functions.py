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


def nonary():
    return 1


def unary(x: int):
    return x + 1


def binary(x: int, y: float):
    return x + y + 1


def ternary(x: int, y: int, z: int = 0):
    return x + y + z + 1


def nary(*args):
    return sum(args)


nonary_lambda = lambda: 1
unary_lambda = lambda x: x + 1
binary_lambda = lambda x, y: x + y + 1
ternary_lambda = lambda x, y, z=0: x + y + z + 1


example_functions = [nonary, unary, binary, ternary, nary]
example_lambdas = [
    nonary_lambda,
    unary_lambda,
    binary_lambda,
    ternary_lambda,
]

example_callables = example_functions + example_lambdas
