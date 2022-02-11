from pathlib import Path

from dbgen import Environment, Import

ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"

DEFAULT_ENV = Environment([Import("typing", ["List", "Tuple"])])
