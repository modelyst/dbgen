# External imports
from setuptools import setup # type: ignore
# Internal Imports


setup(
    name="dbgen",
    entry_points = {
        'airflow.plugins': [
            'dbgen_plugin = dbgen.core.airflow_plugin:DBgenPlugin'
        ]
    }
)
