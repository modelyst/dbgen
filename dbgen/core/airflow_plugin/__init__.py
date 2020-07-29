# External imports
from airflow.plugins_manager import AirflowPlugin

# Internal Imports

# Operators
from .genoperator import GenOperator


# Defining the plugin class
class DBgenPlugin(AirflowPlugin):
    name = "dbgen_plugin"
    operators = [GenOperator]
