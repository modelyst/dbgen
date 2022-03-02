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

import pytest
from pydantic import PostgresDsn
from pydantic.tools import parse_obj_as

from dbgen.core.node.query import Connection
from tests.example.database import dsn

test_urls = (
    "postgresql://admin:password@address:9999/test_db",
    "postgresql://admin:password_1@address:9999/test_db",
)
test_connection = Connection.from_uri(dsn)


@pytest.fixture()
def test_dsn():
    test = parse_obj_as(PostgresDsn, test_urls[0])
    assert test.user == "admin"
    assert test.password == "password"
    assert test.port == "9999"
    assert test.path == "/test_db"
    return test


def test_connection_instantation(test_dsn: PostgresDsn):
    connection = Connection.from_uri(test_dsn)
    assert connection.scheme == "postgresql"
    assert connection.dict(by_alias=True).get("schema") == "public"
    assert connection.host == "address"
    assert connection.database == "test_db"
    assert connection.password is not None
    assert connection.password.get_secret_value() == "password"
    assert connection.port == 9999


def test_connection_hashing():
    connection_1 = Connection.from_uri(test_urls[0])
    connection_2 = Connection.from_uri(test_urls[0].replace("password", "admin"))
    assert connection_1.hash == connection_2.hash


def test_get_engine_from_connection():
    from sqlalchemy import create_engine

    connection = Connection.from_uri(test_urls[0])
    create_engine(
        connection.url(),
        connect_args={"options": f"-csearch_path={connection.schema_}"},
    )


@pytest.mark.database
def test_test_connection():
    assert test_connection.test()
    bad_conn = Connection.from_uri("postgresql://not_existing:@localhost/dbgen")
    assert not bad_conn.test()
