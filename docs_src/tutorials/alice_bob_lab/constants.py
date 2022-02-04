from os.path import abspath, dirname, join

from dbgen import Env, Import

ROOT_DIR = abspath(dirname(__file__))
DATA_DIR = join(ROOT_DIR, "data")

DEFAULT_ENV = Env([Import("typing", ["List", "Tuple"])])
