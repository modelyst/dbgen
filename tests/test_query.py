#   Copyright 2022 Modelyst LLC
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

from dbgen.core.args import Arg
from dbgen.core.node.query import BaseQuery, Connection, ExternalQuery
from tests.example.database import dsn

test_connection = Connection.from_uri(dsn)
test_engine = test_connection.get_engine()


@pytest.mark.database
def test_base_query(seed_db):
    ext = BaseQuery(query="Select 1 as test", outputs=["test"])
    engine = test_connection.get_engine()
    with engine.connect() as conn:
        ext.set_connection(connection=conn)
        output = list(ext.extract())
        assert len(output)
        assert all(map(lambda x: "test" in x, output))
        assert isinstance(ext["test"], Arg)


@pytest.mark.database
def test_external_connection(seed_db):
    ext = ExternalQuery(query="Select 1 as test", outputs=["test"], connection=test_connection)
    with ext:
        output = list(ext.extract())
        assert len(output)
        assert all(map(lambda x: "test" in x, output))
    ext = ExternalQuery(
        query="Select name from users order by id asc",
        outputs=["test"],
        connection=test_connection,
    )
    with ext:
        output = list(ext.extract())
        assert len(output) == 100
        assert all(map(lambda x: "name" in x, output))
        assert output[0] == {"name": "user_0"}


@pytest.mark.database
def test_external_connection_streaming(seed_db):
    ext = ExternalQuery(
        query="Select name from users order by id asc",
        outputs=["name"],
        connection=test_connection,
    )
    ext._yield_per = 10
    with ext:
        for row in ext.extract():
            assert "name" in row
    with ext:
        ext._yield_per = 10
        output = list(ext.extract())
        assert len(output) == 100
        assert all(map(lambda x: "name" in x, output))
