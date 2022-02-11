from pathlib import Path

from dbgen import Env, Import

ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"

DEFAULT_ENV = Env([Import("typing", ["List", "Tuple"])])
