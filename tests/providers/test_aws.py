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

# S3 Extractor
import json
import os
import sys
from importlib import import_module, reload
from tempfile import NamedTemporaryFile
from unittest import mock

import boto3
import pytest
from moto import mock_s3

from dbgen.exceptions import MissingImportError
from dbgen.providers.aws.extract import S3FileExtractor, S3FileNameExtractor


@pytest.fixture
def bucket_name():
    return "testing_bucket_name"


@pytest.fixture
def region():
    return "us-west-2"


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3_client(aws_credentials, region):
    with mock_s3():
        conn = boto3.client("s3", region_name=region)
        yield conn


@pytest.fixture
def mock_s3_bucket(s3_client, bucket_name, region):
    location = {"LocationConstraint": region}
    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    dictionaries = [{"a": 1}, {"b": 2}]
    file_contents = []
    file_names = []
    for dictionary in dictionaries:
        current_file_contents = json.dumps(dictionary)
        file_contents.append(current_file_contents)
        with NamedTemporaryFile(delete=True, suffix=".json") as tmp:
            file_names.append(tmp.name)
            with open(tmp.name, "w") as f:
                f.write(current_file_contents)
            s3_client.upload_file(tmp.name, bucket_name, tmp.name)

    return file_names, file_contents


def test_s3_file_name(mock_s3_bucket, bucket_name):
    extractor = S3FileNameExtractor(bucket=bucket_name)
    file_dict = {name: content for name, content in zip(*mock_s3_bucket)}
    for key in extractor.extract():
        assert key in file_dict


def test_s3_extractor(mock_s3_bucket, bucket_name):
    extractor = S3FileExtractor(bucket=bucket_name)
    file_dict = {name: content for name, content in zip(*mock_s3_bucket)}
    for key, contents in extractor.extract():
        assert key in file_dict
        assert contents == file_dict[key]


def test_missing_boto3():
    """Ensure helpful error is thrown when boto3 is installed and aws is accessed"""
    with pytest.raises(MissingImportError):
        with mock.patch.dict(sys.modules):
            sys.modules["boto3"] = None
            if "dbgen.providers.aws" in sys.modules:
                reload(sys.modules["dbgen.providers.aws"])
            else:
                import_module("dbgen.providers.aws")
