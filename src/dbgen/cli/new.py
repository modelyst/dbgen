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

import configparser

# External imports
import sys
from argparse import ArgumentParser
from os import environ, mkdir, system
from os.path import abspath, dirname, exists, join
from shutil import copyfile, rmtree
from venv import create  # type: ignore

from dbgen import __file__ as DBGEN_ROOT

"""
Initialize a dbgen model

PROBLEMS

-path to .env and /data/ folders should be able to be specified (or by default be
    parallel )
- don't use DBGEN_ROOT -- use '__file__' instead to get current dbgen repo
"""
################################################################################

# Check to confirm that python3.6 or newer is being used
major_version, minor_version = sys.version_info[:2]
if major_version < 3 or minor_version < 6:
    raise Exception("Python 3.6+ is required.")

root = abspath(join(dirname(DBGEN_ROOT), ".."))
user = environ["USER"]
################################################################################


class File:
    def __init__(self, pth: str, template: str, fill_content: bool = True) -> None:
        self.pth = pth
        self.template = template
        self.fill_content = fill_content

    def content(self, kwargs: dict) -> str:
        with open(join(root, "dbgen/CLI/newfiles", self.template)) as f:
            if self.fill_content:
                return f.read().format(**kwargs)
            return f.read()

    def write(self, pth: str, **kwargs: str) -> None:
        with open(join(pth, self.pth), "w") as f:
            f.write(self.content(kwargs))


sch = File("schema.py", "schema.py")
ginit = File("generators/__init__.py", "ginit.py")
default = File("dbgen_files/default.py", "default.py")
io = File("generators/io.py", "io.py")
ana = File("generators/analysis.py", "analysis.py", fill_content=False)
man = File("main.py", "main.py")
data = File("data/example.csv", "data.csv")
parse = File("scripts/io/parse_employees.py", "parse.py")
utils = File("utils.py", "utils.py")
dev, log = [File(f"dbgen_files/{x}.json", x) for x in ["dev", "log"]]

files = [sch, ginit, default, man, io, data, parse, dev, log, utils, ana]
inits = ["", "scripts/", "scripts/io/"]
dirs = [
    "generators",
    "scripts",
    "data",
    "dbgen_files",
    "scripts/io",
    "dbgen_files/storage",
    "dbgen_files/tmp",
]


################################################################################
parser = ArgumentParser(description="Initialize a dbGen model", allow_abbrev=True)
parser.add_argument("--pth", type=str, help="Root folder", required=True)
parser.add_argument("--name", type=str, help="Name of model", required=True)
parser.add_argument("--env", default=".env/bin/activate", type=str, help="Name of model")
parser.add_argument("--create-env", action="store_true", help="Create a virtual env")

################################################################################
def create_config(model_name: str, model_root: str):
    model_root = abspath(model_root)
    envvars = dict(
        MODEL_NAME=model_name,
        MODEL_ROOT=model_root,
        MODEL_TMP=f"{model_root}/dbgen_files/tmp",
    )
    config = configparser.ConfigParser()
    config["dbgen"] = envvars
    with open(join(model_root, "model.cfg"), "w") as f:
        config.write(f)


################################################################################


def main(pth: str, name: str, env: str, create_env: bool = True) -> None:
    """
    Initialize a DbGen model
    #"""
    if exists(pth):
        print(pth, " already exists")
        while True:
            answer = input("Overwrite? (y/n)").lower()
            if answer == "y":
                rmtree(pth)
                break
            elif answer == "n":
                return
            else:
                print("invalid response")

    mkdir(pth)

    for dir in dirs:
        mkdir(join(pth, dir))
    for i in inits:
        system("touch " + join(pth, i + "__init__.py"))
    for fi in files:
        fi.write(pth, model=name, user=user)

    # Create virtual environment
    env = join(pth, env)
    reqs = join(root, "requirements.txt")
    if create_env:
        create(join(pth, ".env"), with_pip=True, symlinks=True, clear=True)
        system("source " + env + "; pip install -r " + reqs)
        copyfile(reqs, join(pth, "requirements.txt"))
    create_config(name, pth)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args.pth, args.name, args.env, args.create_env)
