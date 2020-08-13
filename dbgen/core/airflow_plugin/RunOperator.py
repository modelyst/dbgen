"""Airflow Operator for the intitialization of a run"""
# External imports
from typing import Any, TYPE_CHECKING, Dict as D

if TYPE_CHECKING:
    from dbgen.core.gen import Generator
    from dbgen.utils.sql import Connection as Conn
    from ..misc import ConnectInfo as ConnI
    from ..model.model import Model

    Model
    ConnI
    Conn

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Internal Imports
from ..misc import ConnectInfo as ConnI
from ..model.run_gen import run_gen
from ...utils.exceptions import DBgenGeneratorError


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
        super(RunOperator, self).__init__(task_id="Start Run", **kwargs)
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

    def execute(self, context: Any, gen: "Generator") -> None:

        # Get the db connections
        gcxn = PostgresHook(self.db_conn_id).get_conn()
        mgcxn = PostgresHook(self.mdb_conn_id).get_conn()

        gcxn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        mgcxn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        conn_info = ConnI.from_postgres_hook(
            PostgresHook.get_connection(self.db_conn_id)
        )
        mconn_info = ConnI.from_postgres_hook(
            PostgresHook.get_connection(self.mdb_conn_id)
        )

        assert (
            gen.hash == self.gen_hash
        ), "Serialization error, The gen hash has changed!"

        run_gen_args: D[str, Any] = dict(
            self=None,
            objs=self.objs,
            gen=gen,
            run_id=self.run_id,
            gcxn=gcxn,
            gmcxn=mgcxn,
            conn_info=conn_info,
            mconn_info=mconn_info,
            retry=self.retry,
            serial=self.serial,
            bar=self.bar,
            user_batch_size=self.user_batch_size,
            gen_hash=self.gen_hash,
        )

        err = run_gen(**run_gen_args)
        if err:
            raise DBgenGeneratorError(f"{self.gen_name} failed")
