"""Tests for the ConnectInfo object"""
from hypothesis import strategies as st, given
from dbgen.core.misc import ConnectInfo
from .strategies.utils import letters_complex, letters
from urllib.parse import quote_plus


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
    letters, letters, st.integers(min_value=0, max_value=9999), letters_complex, letters, letters,
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
