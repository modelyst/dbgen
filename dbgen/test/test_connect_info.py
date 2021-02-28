# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for the ConnectInfo object"""
from urllib.parse import quote_plus

from hypothesis import given
from hypothesis import strategies as st

from dbgen.core.misc import ConnectInfo

from .strategies.utils import letters, letters_complex


def test_parse_dsn():
    """Test simple dsn parsing"""
    db_string = "postgresql://user:password@localhost:9999/test"
    conn = ConnectInfo.from_dsn(db_string, schema="testing")
    assert isinstance(conn, ConnectInfo)
    assert conn.db == "test"
    assert conn.user == "user"
    assert conn.host == "localhost"
    assert conn.port == 9999
    assert conn.schema == "testing"
    assert conn.passwd == "password"


@given(
    letters,
    letters,
    st.integers(min_value=0, max_value=9999),
    letters_complex,
    letters,
    letters,
)
def test_parse_dsn_hypo(user: str, host: str, port: int, password: str, schema: str, dbname: str):
    """Hypothesis driven string with basic inputs"""
    db_string = f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{dbname}"
    conn = ConnectInfo.from_dsn(db_string, schema=schema)
    assert isinstance(conn, ConnectInfo)
    assert conn.db == dbname
    assert conn.user == user
    assert conn.host == host
    assert conn.port == port
    assert conn.schema == schema
    assert conn.passwd == password
