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

"""Exceptions used by DBgen"""
from hypothesis.strategies import SearchStrategy, builds
from psycopg2.errors import Error as Psycopg2Error  # type: ignore  # noqa: F401

# Internal Imports
from dbgen.utils.misc import Base


class DBgenException(Exception, Base):
    """
    Base class for all DBgen's errors.
    Each custom exception should be derived from this class
    """

    status_code = 500

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenExternalError(DBgenException):
    """Custom class for catching errors that occur in code external to dbgen"""

    status_code = 400

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenInternalError(DBgenException):
    """
    Custom class for catching errors that occur in code internal to dbgen
    """

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenGeneratorError(DBgenException):
    """Custom class for catching errors that occur in code external to dbgen"""

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenSkipException(DBgenException):
    """Custom class for errors raised when a generator is skipped"""

    def __init__(self, msg) -> None:
        super(self.__class__, self).__init__()
        self.msg = msg

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenNotFoundException(DBgenException):
    """Raise when the requested object/resource is not available in the system"""

    status_code = 404

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenConfigException(DBgenException):
    """Raise when there is configuration problem"""

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenLoadException(DBgenException):
    """Raise when there is loading problem"""

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenMissingInfo(DBgenException):
    """Raise when there is missing info in a class/function initialization"""

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenInvalidArgument(DBgenException):
    """Raise when there is missing info in a class/function initialization"""

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenTypeError(DBgenException):
    """Raise when a value cannot be cast the correct type"""

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)
