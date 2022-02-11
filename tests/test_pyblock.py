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

import pytest
from hypothesis import given
from pydantic import ValidationError

from dbgen.core.args import Arg, Constant
from dbgen.core.func import Environment, Func, func_from_callable
from dbgen.core.node.transforms import PythonTransform
from dbgen.exceptions import DBgenMissingInfo, DBgenPythonTransformError
from tests.example_functions import nonary, ternary
from tests.strategies import pyblock_strat


@given(pyblock_strat)
def test_pyblock_strat(instance: PythonTransform):
    assert isinstance(instance, PythonTransform)
    assert instance.function.nOut


def test_const():
    const = Constant(1)
    assert const.arg_get({}) == 1


def test_pyblock_errors():
    basic_lambda = lambda x: 1
    good_inputs = {"function": basic_lambda, "inputs": [Constant(1)], "outputs": ["test"]}
    PythonTransform(**good_inputs)
    for name, val in (("function", 1), ("inputs", [(1,)]), ("outputs", [])):
        with pytest.raises(ValidationError):
            PythonTransform(**{**good_inputs, name: val})


def test_pyblock_to_dict():
    pb = PythonTransform(env=Environment(imports=[]), function=nonary, inputs=[], outputs=[""])
    serial = pb.dict()
    assert {i: Arg.parse_obj(val) for i, val in enumerate(serial.get("inputs"))} == pb.inputs
    assert Environment.parse_obj(serial.get("env")) == pb.env
    assert serial.get("outputs") == pb.outputs
    assert Func.parse_obj(serial.get("function")) == pb.function
    assert pb.parse_obj(pb.dict()) == pb


def test_pyblock_run():
    pb = PythonTransform(function=nonary, inputs=[], outputs=["out"])
    pb_dict = pb.run({})
    assert "out" in pb_dict
    assert pb_dict["out"] == 1


def test_pyblock_run_with_inputs():
    func = lambda x, y: x + y
    pb = PythonTransform(function=func, inputs=[Constant(1), Constant(3)])
    pb_dict = pb.run({})
    assert "out" in pb_dict
    assert pb_dict["out"] == 4


def test_pyblock_run_with_args():
    func = lambda x, y: x + y
    pb = PythonTransform(function=func, inputs=[Constant(1), Arg(key="other_pyblock", name="out")])
    pb_dict = pb.run({"other_pyblock": {"out": 5}})
    assert "out" in pb_dict
    assert pb_dict["out"] == 6


def test_two_pyblocks():
    func_1 = lambda x: x + 1
    func_2 = lambda x: str(x)
    pb_1 = PythonTransform(function=func_1, inputs=[Constant(1)])
    pb_2 = PythonTransform(function=func_2, inputs=[pb_1["out"]])
    namespace = {}
    for pb in (pb_1, pb_2):
        namespace[pb.hash] = pb.run(namespace)
    assert len(namespace) == 2
    assert namespace[pb_1.hash]["out"] == 2
    assert namespace[pb_2.hash]["out"] == "2"


def test_func():
    func = func_from_callable(ternary)
    assert func.number_of_required_inputs == 2


def test_pyblock_failure_at_runtime():
    func_1 = lambda x: x + 1
    func_2 = lambda x: str(x)
    pb_1 = PythonTransform(function=func_1, inputs=[Constant(1)])
    pb_2 = PythonTransform(function=func_2, inputs=[pb_1["out"]])
    with pytest.raises(DBgenMissingInfo):
        pb_2.run({})

    bad_pb = PythonTransform(function=nonary, inputs=[], outputs=["1", "2"])
    with pytest.raises(DBgenPythonTransformError):
        bad_pb.run({})

    with pytest.raises(ValidationError):
        PythonTransform(function=nonary, inputs=[Constant(1)], outputs=["1", "2"])

    with pytest.raises(ValidationError):
        PythonTransform(function=nonary, inputs=[Constant(1)], outputs=["1", "2"])

    PythonTransform(function=ternary, inputs=[Constant(1), Constant(2)], outputs=["1", "2"])
