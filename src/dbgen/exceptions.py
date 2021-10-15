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


class DBgenException(Exception):
    """
    Base class for all DBgen's errors.
    Each custom exception should be derived from this class
    """

    msg: str = "<No Message>"


class DBgenExternalError(DBgenException):
    """Custom class for catching errors that occur in code external to dbgen"""

    status_code = 400


class DBgenInternalError(DBgenException):
    """
    Custom class for catching errors that occur in code internal to dbgen
    """


class DBgenGeneratorError(DBgenException):
    """Custom class for catching errors that occur in code external to dbgen"""


class DBgenSkipException(DBgenException):
    """Custom class for errors raised when a generator is skipped"""


class DBgenNotFoundException(DBgenException):
    """Raise when the requested object/resource is not available in the system"""

    status_code = 404


class DBgenConfigException(DBgenException):
    """Raise when there is configuration problem"""


class DBgenLoadException(DBgenException):
    """Raise when there is loading problem"""


class DBgenPyBlockError(DBgenException):
    """Raise when there is loading problem"""


class DBgenMissingInfo(DBgenException):
    """Raise when there is missing info in a class/function initialization"""


class DBgenInvalidArgument(DBgenException):
    """Raise when there is missing info in a class/function initialization"""


class DBgenTypeError(DBgenException):
    """Raise when a value cannot be cast the correct type"""


class QueryParsingError(DBgenException):
    """Error raised when parsing a sqlalchemy select statement."""