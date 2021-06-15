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

# External imports
from typing import TYPE_CHECKING, Any
from typing import Dict as D
from typing import List as L
from typing import Tuple as T

if TYPE_CHECKING:
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
from dbgen.core.gen import Generator
from dbgen.core.misc import ConnectInfo as ConnI
from dbgen.core.model.run_gen import run_gen
from dbgen.utils.exceptions import DBgenGeneratorError
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
        super().__init__(**kwargs)  # type: ignore
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

    def _get_gen(self, mcxn: "Conn") -> Generator:
        get_a = mkSelectCmd("gen", ["gen_json"], ["gen_id"])
        gen_json = sqlselect(mcxn, get_a, [self.gen_hash])[0][0]
        gen = Generator.fromJSON(gen_json)  # type: ignore
        return gen  # type: ignore

    def execute(self, context: Any) -> None:

        # Get the db connections
        gcxn = PostgresHook(self.db_conn_id).get_conn()
        mgcxn = PostgresHook(self.mdb_conn_id).get_conn()

        gcxn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        mgcxn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        conn_info = ConnI.from_postgres_hook(PostgresHook.get_connection(self.db_conn_id))
        mconn_info = ConnI.from_postgres_hook(PostgresHook.get_connection(self.mdb_conn_id))

        gen = self._get_gen(mgcxn)
        assert gen.hash == self.gen_hash, "Serialization error, The gen hash doesn't has changed!"

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
            raise DBgenGeneratorError(f"{self.gen_name} failed")
