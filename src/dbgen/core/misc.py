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

"""Uncategorized utilities for core dbgen functionality"""
import logging
import re
from contextlib import suppress
from json import dump, load
from os import environ
from os.path import exists
from pprint import pformat

# External Modules
from typing import TYPE_CHECKING, Any
from typing import Callable as C
from typing import List as L
from typing import Tuple as T

from hypothesis.strategies import SearchStrategy, builds
from psycopg2 import Error, OperationalError, connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, parse_dsn

from dbgen.utils.aws import get_secret
from dbgen.utils.misc import Base

# Internal Modules
if TYPE_CHECKING:
    from airflow.hooks.connections import Connection
    from sshtunnel import SSHTunnelForwarder

    from dbgen.core.gen import Generator

logger = logging.getLogger(__name__)


class ConnectInfo(Base):
    """
    PostGreSQL connection info
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5432,
        user: str = None,
        passwd: str = None,
        db: str = "",
        schema: str = "public",
        ssh: str = "",
        ssh_port: int = 22,
        ssh_username: str = "",
        ssh_pkey: str = "",
        remote_bind_address: str = "localhost",
        remote_bind_port: int = 5432,
    ) -> None:
        """
        Create a ConnectInfo object to wrap around postgres connection information.

        Args:
            host (str, optional): hostname/ip address for postgres server. Defaults to "127.0.0.1".
            port (int, optional): port number postgres server is listening on. Defaults to 5432.
            user (str, optional): username to login to postgres with. Defaults to None.
            passwd (str, optional): password for above user. Defaults to None.
            db (str, optional): database to login to. Defaults to "".
            schema (str, optional): set the schema to search. Defaults to "public".
            ssh (str, optional): ssh hostname to tunnel through. Defaults to "".
            ssh_port (int, optional): port the host is using to listen to ssh connections. Defaults to 22.
            ssh_username (str, optional): username to log onto ssh tunnel. Defaults to "".
            ssh_pkey (str, optional): password to log onto the ssh tunnel. Defaults to "".
            remote_bind_address (str, optional): what host to query on ssh machine. Defaults to "localhost".
            remote_bind_port (int, optional): what port postgres is listening to
            on ssh machine. Defaults to 5432.
        """

        if not user:
            user = passwd = environ.get("USER", "")

        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.schema = schema
        self.ssh = ssh
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_pkey = ssh_pkey
        self.remote_bind_address = remote_bind_address
        self.remote_bind_port = remote_bind_port
        super().__init__()

    def __str__(self) -> str:
        return pformat(self.__dict__)

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)

    def tunnel(self) -> "SSHTunnelForwarder":
        from sshtunnel import SSHTunnelForwarder

        return (
            SSHTunnelForwarder(
                (self.ssh, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=self.ssh_pkey,
                remote_bind_address=(
                    self.remote_bind_address,
                    self.remote_bind_port,
                ),
            )
            if self.ssh
            else suppress()
        )

    def connect(self, attempt: int = 3, auto_commit: bool = True) -> "Connection":
        for _ in range(attempt):
            try:
                with self.tunnel():
                    conn = connect(
                        host=self.host,
                        port=self.port,
                        user=self.user,
                        password=self.passwd,
                        dbname=self.db,
                        connect_timeout=28800,
                    )
                    if auto_commit:
                        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    if self.schema != "public":
                        cur = conn.cursor()
                        cur.execute(f'SET search_path TO "{self.schema}"')
                    return conn
            except OperationalError as exc:
                if re.findall("database.*does not exist", str(exc)):
                    raise OperationalError(
                        f"Database {self.db} does not exist, "
                        "please check connection or create DB before running DBgen or first time"
                    )
                raise exc
        raise Error(
            "Exceeded number of attempts to connect to host using credentials."
            "Please make sure the database is running and you have provided the correct credentials."
        )

    def to_file(self, pth: str) -> None:
        """Store connectinfo data as a JSON file"""
        with open(pth, "w") as f:
            dump(vars(self), f)

    def to_dsn(self, with_password: bool = False) -> "str":
        """
        Create from dsn for current connection info
        """
        passwd = self.passwd if with_password else ""
        dsn = f"postgresql://{self.user}:{passwd}@{self.host}:{self.port}/{self.db}?options=--search_path%3d{self.schema}"
        return dsn

    @staticmethod
    def from_file(pth: str) -> "ConnectInfo":
        """
        Create from path to file with ConnectInfo fields in JSON format
        """
        assert exists(pth), "Error loading connection info: no file at " + pth
        with open(pth) as f:
            return ConnectInfo(**load(f))

    @staticmethod
    def from_dsn(dsn: str, schema: str = None) -> "ConnectInfo":
        """
        Create from path to file with ConnectInfo fields in JSON format
        """
        parsed_dsn = parse_dsn(dsn)
        kwargs = dict(
            user=parsed_dsn["user"],
            db=parsed_dsn["dbname"],
            host=parsed_dsn["host"],
            port=int(parsed_dsn.get("port", "5432")),
            passwd=parsed_dsn.get("password"),
        )
        if schema:
            kwargs["schema"] = schema
        return ConnectInfo(**kwargs)

    @staticmethod
    def from_aws_secret(
        secret_id: str, region: str, profile: str, schema: str = None, host: str = None, port: int = None
    ) -> "ConnectInfo":
        """
        Create from path to file with ConnectInfo fields in JSON format
        """
        secret = get_secret(secret_id, region_name=region, profile_name=profile)
        secret_kwargs = dict(
            user=secret["username"],
            db=secret["dbname"],
            host=secret["host"],
            port=int(secret.get("port", "5432")),
            passwd=secret.get("password"),
        )
        if schema:
            secret_kwargs["schema"] = schema
        if host:
            secret_kwargs["host"] = host
        if port:
            secret_kwargs["port"] = int(port)
        return ConnectInfo(**secret_kwargs)

    @staticmethod
    def from_postgres_hook(airflow_connection: "Connection") -> "ConnectInfo":
        """
        Create from path to file with ConnectInfo fields in JSON format
        """
        kwargs = dict(
            host=airflow_connection.host,
            port=airflow_connection.port,
            user=airflow_connection.login,
            passwd=airflow_connection.get_password(),
            db=airflow_connection.schema,
        )
        return ConnectInfo(**kwargs)

    def neutral(self) -> "Connection":
        copy = self.copy()
        copy.db = "postgres"
        conn = copy.connect()
        return conn.cursor()

    def kill(self) -> None:
        """Kills connections to the DB"""
        killQ = """SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s
                      AND pid <> pg_backend_pid();"""
        with self.neutral() as cxn:
            cxn.execute(killQ, vars=[self.db])

    def drop(self) -> None:
        """Completely removes a DB"""
        drop_stmt = f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE'
        conn = self.connect()
        with conn.cursor() as cxn:
            cxn.execute(drop_stmt)

    def create(self) -> None:
        """Kills connections to the DB"""
        create_stmt = f'CREATE SCHEMA IF NOT EXISTS "{self.schema}";'
        conn = self.connect()
        with conn.cursor() as cxn:
            cxn.execute(create_stmt)

    def test_connection(self, with_password: bool = False, verbose: bool = False) -> bool:
        if verbose:
            print(f"Testing ConnectInfo: {self.to_dsn(with_password)}")
        try:
            cxn = self.connect()
            with cxn.cursor() as curs:
                curs.execute("Select 1;")
                output = curs.fetchone()
                assert output[0] == 1, "Error running simple Query!"
        except OperationalError as exc:
            print("Cannot connect to database!")
            print(f"{exc}")
            return False
        return True


################################################################################


class Dep(Base):
    """
    Capture dependency information between two Generators that modify a DB
    through four different sets: the tabs/cols that are inputs/outputs.
    """

    def __init__(
        self,
        tabs_needed: L[str] = [],
        cols_needed: L[str] = [],
        tabs_yielded: L[str] = [],
        cols_yielded: L[str] = [],
    ) -> None:
        """
        Initialize the Dep object with the tables and columns needed and yielded
        by a generator.

        Args:
            tabs_needed (L[str], optional): The tables the generator queries from. Defaults to [].
            cols_needed (L[str], optional): The columns the generator queries. Defaults to [].
            tabs_yielded (L[str], optional): The tables the generator loads into. Defaults to [].
            cols_yielded (L[str], optional): The columns the generator loads into. Defaults to [].
        """
        allts = [tabs_needed, tabs_yielded]
        allcs = [cols_needed, cols_yielded]
        assert all([all(["." not in t for t in ts]) for ts in allts]), allts
        assert all([all(["." in c for c in cs]) for cs in allcs]), allcs
        self.tabs_needed = set(tabs_needed)
        self.cols_needed = set(cols_needed)
        self.tabs_yielded = set(tabs_yielded)
        self.cols_yielded = set(cols_yielded)
        super().__init__()

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)

    def all(self) -> T[str, str, str, str]:
        a, b, c, d = tuple(
            map(
                lambda x: ",".join(sorted(x)),
                [
                    self.tabs_needed,
                    self.cols_needed,
                    self.tabs_yielded,
                    self.cols_yielded,
                ],
            )
        )
        return a, b, c, d

    def __str__(self) -> str:
        return pformat(self.__dict__)

    def __bool__(self) -> bool:
        return bool(self.tabs_needed | self.cols_needed | self.tabs_yielded | self.cols_yielded)

    # Public Methods #

    def test(self, other: "Dep") -> bool:
        """Test whether SELF depends on OTHER"""
        return not (
            self.tabs_needed.isdisjoint(other.tabs_yielded)
            and self.cols_needed.isdisjoint(other.cols_yielded)
        )

    @classmethod
    def merge(cls, deps: L["Dep"]) -> "Dep":
        """Combine a list of Deps using UNION"""
        tn, cn, ty, cy = set(), set(), set(), set()  # type: ignore
        for d in deps:
            tn = tn | d.tabs_needed
            cn = cn | d.cols_needed
            ty = ty | d.tabs_yielded
            cy = cy | d.cols_yielded
        return cls(tn, cn, ty, cy)  # type: ignore


################################################################################
class Test:
    """
    Execute a test before running action. If it returns True, the test is
    passed, otherwise it returns an object which is fed into the "message"
    function. This prints a message: "Not Executed (string of object)"
    """

    def __init__(self, test: C[["Generator", Any], bool], message: C[[Any], str]) -> None:
        """
        Initialize test object with callable that takes in a generator and
        returns a bool and a helpful error message.

        Args:
            test (Callable[["Generator",Any]],bool]): Callable that takes in a generator and returns a bool.
            message (Callable[[Any], str]): function that returns a helpful error using the output of the generator.
        """
        self.test = test
        self.message = message

    def __call__(self, t: "Generator", *args: Any) -> Any:
        """Run a test on a generator to see if it's supposed to be executed"""
        output = self.test(t, *args)
        return True if output else self.message(output)


#################
# Example Tests #
#################

onlyTest = Test(
    lambda t, o: (len(o) == 0) or (t.name in o) or any([g in t.tags for g in o]),  # type: ignore
    lambda x: "excluded",
)

xTest = Test(
    lambda t, x: (t.name not in x) and (not any([g in t.tags for g in x])),  # type: ignore
    lambda x: "excluded",
)
