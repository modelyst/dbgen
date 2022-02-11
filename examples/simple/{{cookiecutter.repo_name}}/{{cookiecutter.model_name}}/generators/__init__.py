from dbgen import Model

{% if cookiecutter.clean=="false" %}# Add generators here
from .io import add_io_gens


def add_generators(model: Model):
    add_io_gens(model)
{% else %}def add_generators(model: Model):
    # Add generators here
    pass
{% endif %}
