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

import logging

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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DummyGenOperator(BaseOperator):
    """A custom airflow operator for the generator object"""

    template_fields = ()
    ui_color = "#b3cde0"

    @apply_defaults
    def __init__(
        self,
        objs: D[str, T[L[str], L[int], L[int]]],
        gen_name: str,
        gen_hash: int,
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
        self.retry = retry
        self.serial = serial
        self.bar = bar
        self.user_batch_size = user_batch_size
        self.db_conn_id = db_conn_id
        self.mdb_conn_id = mdb_conn_id
        self._logger = logging.getLogger(f"dbgen.run.{gen_name}")

    def execute(self, context: Any) -> None:

        # Get the db connections
        self._logger.info("Dummy Generator Executing...")
        self._logger.info(f"Running Generator {self.gen_name}")
        self._logger.info(f"{self.objs=}")
        self._logger.info(f"{self.gen_name=}")
        self._logger.info(f"{self.gen_hash=}")
        self._logger.info(f"{self.retry=}")
        self._logger.info(f"{self.serial=}")
        self._logger.info(f"{self.bar=}")
        self._logger.info(f"{self.user_batch_size=}")
        self._logger.info(f"{self.db_conn_id=}")
        self._logger.info(f"{self.mdb_conn_id=}")
        self._logger.info(f"Done!")
