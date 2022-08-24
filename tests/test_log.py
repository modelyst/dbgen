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

import logging

from dbgen.core.base import Base


def test_logger(caplog):
    with caplog.at_level(logging.DEBUG, logger="dbgen"):

        class Dummy(Base):
            attr_1: str = 1
            _logger_name = lambda _, kwargs: f"dbgen.dummy.Dummy({kwargs.get('attr_1',1)})"

        dummy = Dummy(attr_1=1)
        dummy._logger.info("Test")
        dummy = Dummy(attr_1=2)
        dummy._logger.info("Testing Dummy 2")
        captured = caplog.text.strip()
        lines = captured.split("\n")
        assert len(lines) == 2
        line_1, line_2 = lines
        assert "dbgen.dummy.Dummy(1)" in line_1
        assert "dbgen.dummy.Dummy(2)" in line_2


def test_log_to_stdout(capfd):
    """Test that the logs are sent to stdout with level INFO by default"""
    message = 'test message'
    test_logger = logging.getLogger('dbgen.test')
    for method in (test_logger.info, test_logger.warning, test_logger.error):
        method(message)
        out, err = capfd.readouterr()
        assert message in out
        assert err == ''
    test_logger.debug(message)
    out, err = capfd.readouterr()
    assert out == ''
    assert err == ''
