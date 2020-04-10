# External
from os import environ
from os.path import join

# INternal
from dbgen import Model, Gen, Const, PyBlock, Env, Import, defaultEnv
from dbgen.example.scripts.parsers import (
    parse_ssn,
    parse_proc_csv,
    parse_expt,
    get_electrode,
    parse_sqlite,
)

############################################################################
############################################################################
########################################################################

root = join(environ["DBGEN_ROOT"], "dbgen/example/data/")

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
        outnames=["firstname", "lastname", "ssn"],
    )

    scientists = Gen(
        name="scientists",
        desc="populates Scientist table",
        funcs=[pb1],
        actions=[
            Scientist(
                insert=True,
                ssn=pb1["ssn"],
                firstname=pb1["firstname"],
                lastname=pb1["lastname"],
            )
        ],
    )

    ############################################################################

    dd_env = defaultEnv + Env(
        Import("collections", "defaultdict"), Import("csv", "DictReader")
    )

    ghcpb = PyBlock(
        func=parse_proc_csv,
        env=dd_env,
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

    sample_action = Sample(insert=True, id=ghcpb["id"])

    proc_action = Procedures(insert=True, procedure_name=ghcpb["procedure_name"])

    sci_action = Scientist(insert=True, ssn=ghcpb["ssn"])

    hist_action = History(
        insert=True,
        step=ghcpb["step"],
        sample=sample_action,
        expt_type=proc_action,
        operator=sci_action,
    )

    histd_action = History_details(
        insert=True,
        name=ghcpb["name"],
        value=ghcpb["value"],
        dtype=ghcpb["dtype"],
        history=hist_action,
    )
    get_history_csv = Gen(
        name="get_history_csv",
        desc="Parse CSV file with History data",
        funcs=[ghcpb],
        actions=[histd_action],
    )

    ############################################################################

    ca = []

    for x in ["Cathode", "Anode"]:
        capb = PyBlock(
            func=get_electrode,
            args=[Const(root + "experiment.json"), Const(x)],
            outnames=["id", "expt_id", "comp"],
        )

        samp_action = Sample(insert=True, id=capb["id"])

        elec_action = Electrode(
            insert=True, composition=capb["comp"], sample=samp_action
        )

        x_action = model[x](insert=True, electrode=elec_action)

        fc_action = Fuel_cell(insert=True, **{x: x_action, "expt_id": capb["expt_id"]})
        ca.append(
            Gen(
                name=x,
                desc="Extract %s info from an experiment.json" % x,
                funcs=[capb],
                actions=[fc_action],
            )
        )

    cathode, anode = ca
    ############################################################################
    sqlite_env = defaultEnv + Env(Import("sqlite3", "connect"))

    ghd = PyBlock(
        parse_sqlite,
        env=sqlite_env,
        args=[Const(root + "procedure.db")],
        outnames=["id", "step", "pname", "fname", "lname", "ssn"],
    )

    samact = Sample(insert=True, id=ghd["id"])

    sciact = Scientist(
        insert=True, ssn=ghd["ssn"], firstname=ghd["fname"], lastname=ghd["lname"]
    )

    proact = Procedures(insert=True, procedure_name=ghd["pname"])

    hact = History(
        insert=True, step=ghd["step"], sample=samact, expt_type=proact, operator=sciact
    )

    get_history_db = Gen(
        name="get_history_db",
        desc="Parse SQLite file with History data",
        funcs=[ghd],
        actions=[hact],
    )
    ############################################################################
    details = ["expt_id", "timestamp", "capacity", "electrolyte"]
    fd_pb = PyBlock(
        parse_expt, args=[Const(root + "experiment.json")], outnames=details
    )
    fuel_details = Gen(
        name="fuel_details",
        desc="other details about fuel cell experiments",
        actions=[Fuel_cell(insert=True, **{x: fd_pb[x] for x in details})],
        funcs=[fd_pb],
    )

    ############################################################################
    ############################################################################

    model.add(
        [scientists, get_history_csv, cathode, anode, get_history_db, fuel_details]
    )
