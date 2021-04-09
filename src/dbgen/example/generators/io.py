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

# External
from os.path import dirname, join

# INternal
from dbgen import Const, Env, Generator, Import, Model, PyBlock, __file__
from dbgen.example.constants import defaultEnv
from dbgen.example.scripts.parsers import get_electrode, parse_expt, parse_proc_csv, parse_sqlite, parse_ssn

############################################################################
############################################################################
########################################################################
root = join(dirname(__file__), "example/data/")

################################################################################
############################################################################
############################################################################


def io(model: Model) -> None:

    tabs = [
        "sample",
        "scientist",
        "procedures",
        "history_detail",
        "history",
        "electrode",
        "anode",
        "cathode",
        "fuel_cell",
    ]

    (
        Sample,
        Scientist,
        Procedures,
        History_details,
        History,
        Electrode,
        Anode,
        Cathode,
        Fuel_cell,
    ) = map(model.get, tabs)

    ############################################################################
    ############################################################################

    pb1 = PyBlock(
        func=parse_ssn,
        args=[Const(root + "ssn.json")],
        env=defaultEnv,
        outnames=["firstname", "lastname", "ssn"],
    )

    scientists = Generator(
        name="scientists",
        desc="populates Scientist table",
        transforms=[pb1],
        loads=[
            Scientist(
                insert=True,
                ssn=pb1["ssn"],
                firstname=pb1["firstname"],
                lastname=pb1["lastname"],
            )
        ],
    )

    ############################################################################

    dd_env = Env([Import("collections", "defaultdict"), Import("csv", "DictReader")])

    ghcpb = PyBlock(
        func=parse_proc_csv,
        env=defaultEnv + dd_env,
        args=[Const(root + "procedures.csv")],
        outnames=[
            "id",
            "step",
            "procedure_name",
            "timestamp",
            "ssn",
            "value",
            "dtype",
            "name",
        ],
    )

    sample_load = Sample(insert=True, id=ghcpb["id"])

    proc_load = Procedures(insert=True, procedure_name=ghcpb["procedure_name"])

    sci_load = Scientist(insert=True, ssn=ghcpb["ssn"])

    hist_load = History(
        insert=True,
        step=ghcpb["step"],
        sample=sample_load,
        expt_type=proc_load,
        operator=sci_load,
    )

    histd_load = History_details(
        insert=True,
        name=ghcpb["name"],
        value=ghcpb["value"],
        dtype=ghcpb["dtype"],
        history=hist_load,
    )
    get_history_csv = Generator(
        name="get_history_csv",
        desc="Parse CSV file with History data",
        transforms=[ghcpb],
        loads=[histd_load],
    )

    ############################################################################

    ca = []

    for x in ["Cathode", "Anode"]:
        capb = PyBlock(
            func=get_electrode,
            env=defaultEnv,
            args=[Const(root + "experiment.json"), Const(x)],
            outnames=["id", "expt_id", "comp"],
        )

        samp_load = Sample(insert=True, id=capb["id"])

        elec_load = Electrode(insert=True, composition=capb["comp"], sample=samp_load)

        x_load = model[x](insert=True, electrode=elec_load)

        fc_load = Fuel_cell(insert=True, **{x: x_load, "expt_id": capb["expt_id"]})
        ca.append(
            Generator(
                name=x,
                desc=f"Extract {x} info from an experiment.json",
                transforms=[capb],
                loads=[fc_load],
            )
        )

    cathode, anode = ca
    ############################################################################
    sqlite_env = Env([Import("sqlite3", "connect")])

    ghd = PyBlock(
        parse_sqlite,
        env=defaultEnv + sqlite_env,
        args=[Const(root + "procedure.db")],
        outnames=["id", "step", "pname", "fname", "lname", "ssn"],
    )

    samact = Sample(insert=True, id=ghd["id"])

    sciact = Scientist(
        insert=True,
        ssn=ghd["ssn"],
        firstname=ghd["fname"],
        lastname=ghd["lname"],
    )

    proact = Procedures(insert=True, procedure_name=ghd["pname"])

    hact = History(
        insert=True,
        step=ghd["step"],
        sample=samact,
        expt_type=proact,
        operator=sciact,
    )

    get_history_db = Generator(
        name="get_history_db",
        desc="Parse SQLite file with History data",
        transforms=[ghd],
        loads=[hact],
    )
    ############################################################################
    details = ["expt_id", "timestamp", "capacity", "electrolyte"]
    fd_pb = PyBlock(parse_expt, env=defaultEnv, args=[Const(root + "experiment.json")], outnames=details)
    fuel_details = Generator(
        name="fuel_details",
        desc="other details about fuel cell experiments",
        loads=[Fuel_cell(insert=True, **{x: fd_pb[x] for x in details})],
        transforms=[fd_pb],
    )

    ############################################################################
    ############################################################################

    model.add(
        [
            scientists,
            get_history_csv,
            cathode,
            anode,
            get_history_db,
            fuel_details,
        ]
    )
