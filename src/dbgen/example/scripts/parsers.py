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

from collections import defaultdict
from csv import DictReader
from json import load
from re import findall
from sqlite3 import connect
from typing import Any
from typing import Dict as D
from typing import List as L
from typing import Tuple as T

AnyD = D[str, Any]
################################################################################
chemical_symbols = [
    "X",
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Po",
    "At",
    "Rn",
    "Fr",
    "Ra",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
    "Am",
    "Cm",
    "Bk",
    "Cf",
    "Es",
    "Fm",
    "Md",
    "No",
    "Lr",
    "Rf",
    "Db",
    "Sg",
    "Bh",
    "Hs",
    "Mt",
    "Ds",
    "Rg",
    "Cn",
    "Nh",
    "Fl",
    "Mc",
    "Lv",
    "Ts",
    "Og",
]
# Parse ssn.json
def parse_ssn(pth: str) -> T[L[str], L[str], L[int]]:
    """Expects a JSON file with <<"firstName lastName" : SSN#>> entries"""
    with open(pth) as f:
        data = load(f)
    firstnames, lastnames = map(list, zip(*[d.split() for d in data.keys()]))
    ssns = list(map(int, data.values()))
    return firstnames, lastnames, ssns  # type: ignore


# Parse procedures.csv


def parse_proc_csv(
    pth: str,
) -> T[T[int], T[int], T[str], T[int], T[int], T[str], T[str], T[str]]:
    """Expects CSV: Sample,Step,Procedure,Timestamp,Researcher,Notes"""
    output = defaultdict(list)  # type: AnyD
    with open(pth) as f:
        reader = DictReader(f)
        for row in reader:
            for k, v in row.items():
                output[k].append(v)

    samp_, step_, proc_, time, ssn_, note = map(list, output.values())
    dtype, name = ["str"] * len(samp_), ["notes"] * len(samp_)
    samp, step, ssn = [list(map(int, x)) for x in [samp_, step_, ssn_]]  # type: ignore
    proc = [x.strip() for x in proc_]  # type: ignore
    return samp, step, proc, time, ssn, note, dtype, name  # type: ignore


# Parse experiment.json
def parse_expt(pth: str) -> T[L[int], L[str], L[float], L[str]]:
    """Parse JSON file with experiments containing anode/cathode/capacity/date"""
    expt_ids, dates, capacities, solvents = [], [], [], []
    with open(pth) as f:
        data = load(f).items()

    for expt_id, expt in data:
        expt_ids.append(expt_id)
        dates.append(expt.get("date"))
        solvents.append(expt.get("solvent"))
        capacities.append(expt.get("capacitance"))

    return expt_ids, dates, capacities, solvents  # type: ignore


# Parse experiment.json
def get_electrode(pth: str, x: str) -> T[L[int], L[int], L[str]]:
    """Get either anode or cathode information about all battery experiments"""
    electrode = x.lower()
    assert electrode in ["anode", "cathode"]

    ids, expt_ids, compositions = [], [], []  # type: ignore

    with open(pth) as f:
        data = load(f).items()

    for expt_id, expt in data:
        expt_ids.append(expt_id)
        ids.append(expt[electrode]["id"])
        compositions.append(expt[electrode]["composition"])

    return ids, expt_ids, compositions


# Parse procedure.db
def parse_sqlite(pth: str) -> T[L[int]]:
    """Get sample history data stored in a relational db"""
    db = connect(pth).cursor()
    cols = ",".join(["sample", "step", "procedure", "firstname", "lastname", "ssn"])
    output = db.execute(f"SELECT {cols} FROM test JOIN scientist USING (scientist_id)")
    return tuple(map(list, zip(*output)))  # type: ignore


# Extract element info from a chemical formula string
def parse_formula(formula: str) -> T[L[int], L[str], L[float]]:
    """Parses simple chemical formulas like As2Ga3 and H2O"""
    matches = findall(r"([A-Z][a-z]?)(\d*)", formula)
    symbs, nums = map(list, zip(*matches))  # type: T[L[Any],L[Any]]
    atomic_nums = list(map(chemical_symbols.index, symbs))
    nums = [int(n) if n else 1 for n in nums]
    fracs = [n / sum(nums) for n in nums]
    return atomic_nums, symbs, fracs
