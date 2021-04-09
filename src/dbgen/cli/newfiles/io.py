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

# External modules
from os.path import join

# Internal Modules
from dbgen import Const, Env, Generator, Import, Model, PyBlock

from ..scripts.io.parse_employees import parse_employees  # type: ignore
from ..utils import get_config  # type: ignore

################################################################################


def io(mod: Model) -> None:
    config = get_config()

    # Extract tables
    tabs = ["employee", "department"]

    Emp, Dept = map(mod.get, tabs)

    ################################################################################

    pe_func = PyBlock(
        parse_employees,
        env=Env([Import("csv", "reader")]),
        args=[Const(join(config["dbgen"]["model_root"], "data/example.csv"))],
        outnames=["ename", "sal", "man", "dname", "sec"],
    )

    pop_employees = Generator(
        name="pop_employees",
        desc="parses CSV file with employee info",
        transforms=[pe_func],
        tags=["io"],
        loads=[
            Emp(
                insert=True,
                name=pe_func["ename"],
                salary=pe_func["sal"],
                manager=Emp(insert=True, name=pe_func["man"]),
                department=Dept(
                    insert=True,
                    name=pe_func["dname"],
                ),
            )
        ],
    )

    ################################################################################
    gens = [pop_employees]
    mod.add(gens)
