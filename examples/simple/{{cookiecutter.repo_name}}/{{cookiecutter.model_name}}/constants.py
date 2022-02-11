"""Constants to use within the model."""
{% if cookiecutter.clean=="false" %}from pathlib import Path

from dbgen import Environment, Import

DATA_PATH = Path(__file__).parent.parent / "data"
# Useful env for adding common imports to transforms
DEFAULT_ENV = Environment(imports=[Import('typing',['List','Tuple','Any','Dict','Union'])])
{% endif %}
