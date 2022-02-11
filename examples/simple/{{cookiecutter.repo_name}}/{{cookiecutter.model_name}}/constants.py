"""Constants to use within the model."""
{% if cookiecutter.clean=="false" %}from pathlib import Path

from dbgen import Env, Import

DATA_PATH = Path(__file__).parent.parent / "data"
# Useful env for adding common imports to transforms
DEFAULT_ENV = Env(imports=[Import('typing',['List','Tuple','Any','Dict','Union'])])
{% endif %}
