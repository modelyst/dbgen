# External imports
from setuptools import setup, find_packages  # type: ignore

# Internal Imports
from .

setup(
    name="dbgen",
    version="0.0.2",
    packages=find_packages(),
    entry_points={
        # 'airflow.plugins': [
        #     'dbgen_plugin = dbgen.core.airflow_plugin:DBgenPlugin'
        # ]
    },
)
