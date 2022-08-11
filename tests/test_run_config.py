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

from dbgen.core.etl_step import ETLStep
from dbgen.core.run.utilities import RunConfig

basic_run_config = RunConfig()
run_config = RunConfig(
    include={'a', 'd', 'included'},
    exclude={'b', 'd', 'excluded'},
)
run_config_only_exclude = RunConfig(exclude={'b', 'd'})
run_config_include_only = RunConfig(include={'a', 'd'})

step_a = ETLStep(name='a')
step_b = ETLStep(name='b')
step_c = ETLStep(name='c')
step_d = ETLStep(name='d')


def test_simple_etl_step_should_run():
    # Both Exclude Include
    assert run_config.should_etl_step_run(step_a)
    assert not run_config.should_etl_step_run(step_b)
    assert not run_config.should_etl_step_run(step_c)


def test_no_include_no_exclude_always_true():
    # No Include No exclude
    assert basic_run_config.should_etl_step_run(step_a)
    assert basic_run_config.should_etl_step_run(step_b)
    assert basic_run_config.should_etl_step_run(step_c)
    assert basic_run_config.should_etl_step_run(step_d)


def test_only_exclude():
    # Only Exclude
    assert run_config_only_exclude.should_etl_step_run(step_a)
    assert not run_config_only_exclude.should_etl_step_run(step_b)
    assert run_config_only_exclude.should_etl_step_run(step_c)
    assert not run_config_only_exclude.should_etl_step_run(step_d)


def test_only_include():
    # Only Include
    assert run_config_include_only.should_etl_step_run(step_a)
    assert not run_config_include_only.should_etl_step_run(step_b)
    assert not run_config_include_only.should_etl_step_run(step_c)
    assert run_config_include_only.should_etl_step_run(step_d)


run_config_exclude_with_tags = RunConfig(exclude={'b', 'c', 'excluded'})
excluded_tag_step = ETLStep(name='x', tags=['excluded'])


def test_exclude_with_tags():
    assert run_config_exclude_with_tags.should_etl_step_run(step_a)
    assert not run_config_exclude_with_tags.should_etl_step_run(step_b)
    assert not run_config_exclude_with_tags.should_etl_step_run(step_c)
    assert run_config_exclude_with_tags.should_etl_step_run(step_d)
    assert not run_config_exclude_with_tags.should_etl_step_run(excluded_tag_step)


def test_conflicts():
    """Test that steps that are both excluded and included are not run."""
    step = ETLStep(name='z', tags=['included', 'excluded'])
    assert not run_config.should_etl_step_run(step)
