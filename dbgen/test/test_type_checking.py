# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Test the Load object's type checking"""
import pytest

from dbgen import Attr, ConnectInfo, Const, Gen, Int, Model, Obj, PyBlock
from dbgen.utils.exceptions import DBgenTypeError

int_hashes = {
    1: "-4207971280359554257",
    2: "-6910457630808224182",
    3: "-3296759392465244520",
    4: "-7849403030907192249",
}


@pytest.fixture
def objects():
    parent = Obj("parent", attrs=[Attr("col_int", Int(), identifying=True)])
    return [parent]


@pytest.fixture
def gens(objects):
    parent = objects[0]

    def transform(input_list):
        return [x for x in input_list]

    def get_gen(type_to_check):
        pb = PyBlock(transform, args=[Const([type_to_check(x) for x in [1, 2, 3]])])
        load = parent(insert=True, col_int=pb["out"])
        return Gen(
            f"test_addition_{type_to_check.__name__}",
            loads=[load],
            transforms=[pb],
        )

    return [get_gen(int), get_gen(str), get_gen(float), get_gen(bool)]


@pytest.fixture
def model(objects, gens):
    model = Model("test_model")
    model.add(objects)
    model.add(gens)
    return model


@pytest.mark.parametrize("type_to_check", [int, str, float], ids=["int", "str", "float"])
def test_int_type_checking(model, type_to_check):
    """
    Takes in a model with 1 gen that simply inserts a val of type int checking
    to see failure
    """
    fake_conn = ConnectInfo()
    output_dicts, load_dicts = model.test_gen(
        f"test_addition_{type_to_check.__name__}", fake_conn, interact=False
    )
    assert len(load_dicts) == 1
    print(load_dicts)
    assert all(
        [
            new_row["parent_id"] == int_hashes[int(new_row["col_int"])]
            for new_row in load_dicts[0]["parent_insert"]
        ]
    )


@pytest.mark.parametrize("type_to_check", [bool], ids=["bool"])
def test_bad_int_casting(model, type_to_check):
    fake_conn = ConnectInfo()
    with pytest.raises(DBgenTypeError):
        output_dicts, load_dicts = model.test_gen(
            f"test_addition_{type_to_check.__name__}", fake_conn, interact=False
        )
