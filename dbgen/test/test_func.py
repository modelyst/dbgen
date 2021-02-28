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

from dbgen.core.func import Env

python_headers = [
    """
from abc.lmn import pqr
from abc.lmn import pqr as xyz, txr
import abc
import abc as xyz
""",
    """
from json import loads, dumps, load, dump
from typing import (
    Any,
    Set as S,
    List as L,
    Tuple as T,
    Dict as D,
    Optional as O,
    Callable as C,
    Union as U,
)
""",
]

answers = [
    (
        4,
        (
            ("abc.lmn", "", ["pqr"], {}),
            ("abc.lmn", "", ["txr"], {"pqr": "xyz"}),
            ("abc", "", [], {}),
            ("abc", "xyz", [], {}),
        ),
    ),
    (
        2,
        (
            ("json", "", ["loads", "dumps", "load", "dump"], {}),
            (
                "typing",
                "",
                ["Any"],
                {
                    "Set": "S",
                    "List": "L",
                    "Tuple": "T",
                    "Dict": "D",
                    "Optional": "O",
                    "Callable": "C",
                    "Union": "U",
                },
            ),
        ),
    ),
]


def test_env_from_string():
    """
    Test the Env Parsing
    """
    for header, (num_imports, libs) in zip(python_headers, answers):
        env = Env.from_str(header)
        assert len(env.imports) == num_imports
        for imp, (lib, lib_alias, ua_imps, a_imps) in zip(env.imports, libs):
            assert imp.lib == lib
            assert imp.lib_alias == lib_alias
            assert imp.unaliased_imports == ua_imps
            assert imp.aliased_imports == a_imps
