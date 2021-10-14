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

from dbgen.core.func import Env, Import
from tests.strategies import env_strat, import_strat


def test_basic_import():
    imp = Import(lib="numpy", lib_alias="np")
    assert str(imp) == "import numpy as np"
    imp = Import(lib="numpy", unaliased_imports="random")
    assert str(imp) == "from numpy import random"
    imp = Import(lib="numpy", unaliased_imports="test", aliased_imports={"random": "r"})
    assert str(imp) == "from numpy import test, random as r"
    with pytest.raises(ValidationError):
        Import(lib="numpy", lib_alias="np", unaliased_imports="test")
    with pytest.raises(ValidationError):
        Import(lib="")


@given(import_strat)
def test_import_hypo(instance):
    assert isinstance(instance, Import)
    assert instance == Import.parse_obj(instance.dict())


@given(env_strat)
def test_env_hypo(instance):
    assert isinstance(instance, Env)
    assert instance == Env.parse_obj(instance.dict())
    assert instance.imports == sorted(instance.imports)
