#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Airflow Operator for the intitialization of a run"""
# External imports
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dbgen.core.gen import Generator
    from dbgen.core.misc import ConnectInfo as ConnI
    from dbgen.core.model.model import Model
    from dbgen.utils.sql import Connection as Conn

    Model
    ConnI
    Conn

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Internal Imports
from dbgen.core.misc import ConnectInfo as ConnI


class RunOperator(BaseOperator):
    """A custom airflow operator for starting runs and validating the inputs"""

    template_fields = ()
    ui_color = "#b3cde0"

    @apply_defaults
    def __init__(
        self,
        run_id: int,
        db_conn_id: str,
        mdb_conn_id: str,
        retry: bool = False,
        serial: bool = False,
        bar: bool = False,
        user_batch_size: int = None,
        **kwargs: dict,
    ) -> None:
        super()
        # Initialize variables
        self.run_id = run_id
        self.retry = retry
        self.serial = serial
        self.bar = bar
        self.user_batch_size = user_batch_size
        self.db_conn_id = db_conn_id
        self.mdb_conn_id = mdb_conn_id

    def _get_run_variable(self, mcxn: "Conn") -> "Generator":
        raise NotImplementedError

    def execute(self, context: Any) -> None:

        # Get the db connections
        gcxn = PostgresHook(self.db_conn_id).get_conn()
        mgcxn = PostgresHook(self.mdb_conn_id).get_conn()

        gcxn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        mgcxn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # conn_info = ConnI.from_postgres_hook(PostgresHook.get_connection(self.db_conn_id))
        # mconn_info = ConnI.from_postgres_hook(PostgresHook.get_connection(self.mdb_conn_id))
