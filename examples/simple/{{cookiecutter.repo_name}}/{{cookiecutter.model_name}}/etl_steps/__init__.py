from dbgen import Model

{% if cookiecutter.clean=="false" %}# Add etl_steps here
from .io import add_io_etl_steps


def add_etl_steps(model: Model):
    add_io_etl_steps(model)
{% else %}def add_etl_steps(model: Model):
    # Add etl_steps here
    pass
{% endif %}
