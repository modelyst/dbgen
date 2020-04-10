# External imports
from typing import Any, TYPE_CHECKING, Dict as D, Tuple as T, List as L

if TYPE_CHECKING:
    from dbgen.utils.sql import Connection as Conn
    from ..misc import ConnectInfo as ConnI
    from ..model.model import Model

    Model
    ConnI
    Conn

from airflow.models import BaseOperator  # type: ignore
from airflow.utils.decorators import apply_defaults  # type: ignore
from airflow.hooks.postgres_hook import PostgresHook  # type: ignore
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # type: ignore

# Internal Imports
from ..gen import Gen
from ..model.run_gen import run_gen
from ..misc import ConnectInfo as ConnI, GeneratorError
from dbgen.utils.sql import mkSelectCmd, sqlselect


class GenOperator(BaseOperator):
    """A custom airflow operator for the generator object"""

    template_fields = ()
    ui_color = "#b3cde0"

    @apply_defaults
    def __init__(
        self,
        objs: D[str, T[L[str], L[int], L[int]]],
        gen_name: str,
        gen_hash: int,
        run_id: int,
        db_conn_id: str,
        mdb_conn_id: str,
        retry: bool = False,
        serial: bool = False,
        bar: bool = False,
        user_batch_size: int = None,
        **kwargs: dict,
    ) -> None:
        super(GenOperator, self).__init__(task_id=gen_name, **kwargs)
        # Initialize variables
        self.objs = objs
        self.gen_name = gen_name
        self.gen_hash = gen_hash
        self.run_id = run_id
        self.retry = retry
        self.serial = serial
        self.bar = bar
        self.user_batch_size = user_batch_size
        self.db_conn_id = db_conn_id
        self.mdb_conn_id = mdb_conn_id

    def _get_gen(self, mcxn: "Conn") -> Gen:
        get_a = mkSelectCmd("gen", ["gen_json"], ["gen_id"])
        gen_json = sqlselect(mcxn, get_a, [self.gen_hash])[0][0]
        gen = Gen.fromJSON(gen_json)  # type: ignore
        return gen  # type: ignore

    def execute(self, context: Any) -> None:

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

        gen = self._get_gen(mgcxn)
        assert (
            gen.hash == self.gen_hash
        ), "Serialization error, The gen hash doesn't has changed!"

        run_gen_args = dict(
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

        err = run_gen(**run_gen_args)  # type: ignore
        if err:
            raise GeneratorError(f"{self.gen_name} failed")


if __name__ == "__main__":
    test = GenOperator(Gen(name="test_gen"), run_id=1, db_conn_id="", mdb_conn_id="")
    import pdb

    pdb.set_trace()
