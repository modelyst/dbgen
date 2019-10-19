# External imports
from setuptools import setup, find_packages # type: ignore
# Internal Imports


setup(
    name="dbgen",
    packages = find_packages(),
    entry_points = {
        # 'airflow.plugins': [
        #     'dbgen_plugin = dbgen.core.airflow_plugin:DBgenPlugin'
        # ]
    }
)
