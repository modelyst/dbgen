# External modules
from os.path import join

# Internal Modules
from dbgen import (
    Model,
    Generator,
    PyBlock,
    Env,
    Const,
    Import,
    defaultEnv,
)
from ..utils import get_config  # type: ignore
from ..scripts.io.parse_employees import parse_employees  # type: ignore

################################################################################


def io(mod: Model) -> None:
    config = get_config()

    # Extract tables
    tabs = ["employee", "department"]

    Emp, Dept = map(mod.get, tabs)

    ################################################################################

    pe_func = PyBlock(
        parse_employees,
        env=defaultEnv + Env([Import("csv", "reader")]),
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
                department=Dept(insert=True, name=pe_func["dname"],),
            )
        ],
    )

    ################################################################################
    gens = [pop_employees]
    mod.add(gens)
