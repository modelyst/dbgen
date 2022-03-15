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

from itertools import product

import pytest
from pydantic.tools import parse_obj_as
from sqlalchemy.future import Engine
from sqlmodel import Session, select
from typer.testing import CliRunner

from dbgen import __version__
from dbgen.cli.main import app
from dbgen.configuration import DBgenConfiguration, PostgresqlDsn, config
from tests.example.full_model import Child

runner = CliRunner()


def test_version():
    """Test the dbgen version command"""
    result = runner.invoke(app, ['version'])
    assert __version__ in result.stdout
    result = runner.invoke(app, ['version', '-s'])
    assert __version__ == result.stdout.strip()


@pytest.mark.parametrize('show_passwords,show_defaults', product([True, False], [True, False]))
def test_config(tmpdir, show_passwords: bool, show_defaults: bool):
    """Test the dbgen config command"""
    old_config = tmpdir.mkdir("sub").join("old_config.env")
    old_config.write('# Nothing')
    new_config = tmpdir.join('sub').join("new_config.env")
    cmds = ['config', '-o', new_config, '-c', old_config]
    if show_passwords:
        cmds += ['--show-password']
    if show_defaults:
        cmds += ['--show-defaults']
    runner.invoke(app, cmds)
    assert new_config.read() == DBgenConfiguration().display(True, True)


def test_config_does_not_exist(tmpdir, sql_engine, reset_config_dsn):
    """Test basic use of the dbgen config command."""
    # Check that things error out when an empty config is used
    config.main_dsn = parse_obj_as(PostgresqlDsn, "postgresql://postgres@localhost:5432/dbgen")
    config_file = tmpdir.mkdir("sub").join("config.env")
    results = runner.invoke(app, ['config', '-c', config_file])
    assert results.exit_code == 2
    parsed_lines = list(filter(lambda x: x, results.stdout.strip().split('\n')))
    assert parsed_lines[-1].startswith('Error: Invalid value for \'--config\' / \'-c\': Config file')
    # Check that a default dsn is used when empty config is passed
    original_dsn = config.main_dsn
    config_file.write('# Nothing')
    results = runner.invoke(app, ['config', '-c', config_file])
    assert original_dsn in results.stdout
    assert results.exit_code == 0
    # Set real dsn
    config_file.write(f'# DBgen Settings\ndbgen_main_dsn = {str(sql_engine.url)}\n')
    results = runner.invoke(app, ['config', '-c', config_file])
    assert str(sql_engine.url) in results.stdout
    assert results.exit_code == 0


def test_connect(tmpdir, sql_engine: Engine):
    """Test basic use of the dbgen connect command."""
    config = tmpdir.mkdir("sub").join("config.env")
    config.write(f'# DBgen Settings\ndbgen_main_dsn = {str(sql_engine.url)}')
    results = runner.invoke(app, ['connect', '--test', '-c', config])
    assert results.exit_code == 0


def test_connect_no_test(tmpdir, sql_engine: Engine, reset_config_dsn):
    """Test basic use of the dbgen connect command."""
    config_file = tmpdir.mkdir("sub").join("config_file.env")
    bad_url = 'postgresql://non_existent:not_password@localhost/dbgen'
    bad_config_contents = f'# DBgen Config\ndbgen_main_dsn = {bad_url}'
    config_file.write(bad_config_contents)
    results = runner.invoke(app, ['connect', '-c', config_file])
    assert results.exit_code == 2

    config_file = tmpdir.join("sub").join("config_file_1.env")
    config_file.write(bad_config_contents)
    results = runner.invoke(app, ['connect', '-c', config_file, '--test'])
    assert 'Cannot connect to' in results.stdout
    assert results.exit_code == 2

    config_file.write(f'# DBgen Settings\ndbgen_main_dsn = {str(sql_engine.url)}')
    results = runner.invoke(app, ['connect', '-c', config_file])
    assert results.exit_code == 0


def test_run(tmpdir, sql_engine, reset_config_dsn):
    """Test basic use of the dbgen run command."""
    config_file = tmpdir.mkdir("sub").join("good.env")
    config_file.write(f'# DBgen Settings\ndbgen_main_dsn = {str(sql_engine.url)}')
    results = runner.invoke(app, ['connect', '-c', config_file, '--test'])
    assert results.exit_code == 0

    results = runner.invoke(app, ['run', '--model', 'tests.example.full_model:make_model'])
    assert results.exit_code == 0

    with Session(sql_engine) as session:
        children = session.exec(select(Child)).all()
        assert len(children) == 1002


def test_run_status(tmpdir, sql_engine, reset_config_dsn):
    """Test basic use of the dbgen run command."""
    config_file = tmpdir.mkdir("sub").join("good.env")
    config_file.write(f'# DBgen Settings\ndbgen_main_dsn = {str(sql_engine.url)}')
    results = runner.invoke(app, ['connect', '-c', config_file, '--test'])
    assert results.exit_code == 0
    runner.invoke(app, ['run', 'status'])
    assert results.exit_code == 0
    results = runner.invoke(app, ['run', '--model', 'tests.example.full_model:make_model'])
    assert results.exit_code == 0
    results = runner.invoke(app, ['run', 'status'])
    assert results.exit_code == 0
