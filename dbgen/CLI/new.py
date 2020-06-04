# External imports
import sys
from venv import create  # type: ignore
from os import mkdir, environ, system
from os.path import join, exists, dirname, abspath
from shutil import copyfile
from argparse import ArgumentParser
from dbgen import __file__ as DBGEN_ROOT
import configparser

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


class File(object):
    def __init__(self, pth: str, template: str) -> None:
        self.pth = pth
        self.template = template

    def content(self, kwargs: dict) -> str:
        with open(join(root, "dbgen/CLI/newfiles", self.template), "r") as f:
            return f.read().format(**kwargs)

    def write(self, pth: str, **kwargs: str) -> None:
        with open(join(pth, self.pth), "w") as f:
            f.write(self.content(kwargs))


sch = File("schema.py", "schema.py")
ginit = File("generators/__init__.py", "ginit.py")
default = File("dbgen_files/default.py", "default.py")
io = File("generators/io.py", "io.py")
man = File("main.py", "main.py")
data = File("data/example.csv", "data.csv")
parse = File("scripts/io/parse_employees.py", "parse.py")
utils = File("utils.py", "utils.py")
dev, log = [File("dbgen_files/%s.json" % x, x) for x in ["dev", "log"]]

files = [sch, ginit, default, man, io, data, parse, dev, log, utils]
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
parser.add_argument(
    "--env", default=".env/bin/activate", type=str, help="Name of model"
)

################################################################################
def create_config(model_name: str, model_root: str):
    model_root = abspath(model_root)
    envvars = dict(
        model_name=model_name,
        model_root=model_root,
        model_temp="${model_root}/dbgen_files/tmp",
        default_env="${model_root}/dbgen_files/default.py",
    )  # THIS SHOULD BE AN EMPTY STRING!!!
    config = configparser.ConfigParser()
    config["dbgen_files"] = envvars
    with open(join(model_root, "model.cfg"), "w") as f:
        config.write(f)


################################################################################


def main(pth: str, name: str, env: str, create_env: bool = False) -> None:
    """
    Initialize a DbGen model
    """
    # if exists(pth):
    #     print(pth, " already exists")
    #     return
    # mkdir(pth)

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
    main(args.pth, args.name, args.env)
