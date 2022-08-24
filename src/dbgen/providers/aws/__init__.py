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
from dbgen.exceptions import MissingImportError

# Ensure boto3 is installed for the aws packages
try:
    import boto3  # noqa: F401
except ModuleNotFoundError as exc:
    raise MissingImportError(
        "boto3 is not installed. to use the dbgen.providers.aws package you must install dbgen with the extras [boto3].\n"
        "python -m pip install 'modelyst-dbgen[boto3]'"
    ) from exc

from dbgen.providers.aws.extract import S3FileExtractor, S3FileNameExtractor  # noqa: F401
