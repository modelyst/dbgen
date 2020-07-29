"""Exceptions used by DBgen"""
from hypothesis.strategies import SearchStrategy, builds
from psycopg2.errors import Error as Psycopg2Error  # type: ignore

# Internal Imports
from .misc import Base


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
    """
    Custom class for catching errors that occur in code external to dbgen
    """

    @classmethod
    def _strat(cls) -> SearchStrategy:
        return builds(cls)


class DBgenSkipException(DBgenException):
    """
    Custom class for errors raised when a generator is skipped
    """

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
