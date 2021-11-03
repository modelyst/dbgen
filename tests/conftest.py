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
import sys

import pytest
from psycopg import connect as pg3_connect
from sqlalchemy import MetaData
from sqlmodel import Session, create_engine, text

from dbgen.configuration import config
from dbgen.core.entity import BaseEntity
from dbgen.core.metadata import meta_registry


@pytest.fixture()
def clear_registry():
    # Clear the tables in the metadata for the default base model
    BaseEntity.metadata.clear()
    # Clear the Models associated with the registry, to avoid warnings
    BaseEntity._sa_registry.dispose()
    yield
    BaseEntity.metadata.clear()
    BaseEntity._sa_registry.dispose()


@pytest.fixture(scope="module")
def sql_engine():
    engine = create_engine(config.main_dsn)
    return engine


@pytest.fixture(scope="function")
def connection(sql_engine):
    """sql_engine connection"""
    metadata = MetaData()
    metadata.reflect(sql_engine)
    metadata.drop_all(sql_engine)
    connection = sql_engine.connect()
    yield connection
    connection.close()


@pytest.fixture(scope="function")
def session(connection):
    transaction = connection.begin()
    session = Session(bind=connection, autocommit=False, autoflush=True)
    yield session
    transaction.rollback()
    transaction.close()
    session.close()


@pytest.fixture(scope="function")
def seed_db(connection):
    connection.execute(text("CREATE table users (id serial primary key, name text);"))
    for user in range(100):
        connection.execute(text(f"INSERT into users(name) values ('user_{user}');"))
    connection.commit()
    yield
    connection.execute(text("drop table users;"))
    connection.commit()


@pytest.fixture(scope="function")
def make_db(connection):
    pass

    metadata = MetaData()
    metadata.reflect(connection)
    metadata.drop_all(connection)
    BaseEntity.metadata.create_all(connection)
    connection.commit()
    yield
    BaseEntity.metadata.drop_all(connection)
    connection.commit()


@pytest.fixture(scope="function")
def raw_connection(make_db, sql_engine):
    raw = sql_engine.raw_connection()
    yield raw
    raw.close()


@pytest.fixture(scope="function")
def raw_pg3_connection(make_db, sql_engine):
    connection = pg3_connect(str(sql_engine.url))
    yield connection
    connection.close()


@pytest.fixture
def debug_logger():
    custom_logger = logging.getLogger("dbgen")
    custom_logger.propagate = True
    custom_logger.setLevel(logging.DEBUG)

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s Test"
    formatter = logging.Formatter(log_format)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(formatter)
    custom_logger.addHandler(console_handler)
    return custom_logger


@pytest.fixture(scope='function')
def recreate_meta(connection):
    meta_registry.metadata.drop_all(connection)
    meta_registry.metadata.create_all(connection)
    yield
