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

import csv
import json
import os
from os.path import join
from tempfile import NamedTemporaryFile

import boto3
import pytest
from moto import mock_s3

from dbgen.core.node.common_extractors import CSVExtractor, FileExtractor, S3FileExtractor


@pytest.fixture
def test_dir(tmpdir):
    """Test the file extractor by parsing the local directory"""
    directory = tmpdir.mkdir("sub")
    sub_dir = directory.mkdir("sub")
    for dir_id, curr_dir in enumerate((directory, sub_dir)):
        for i in range(10):
            curr_file = curr_dir.join(f"{dir_id}-{i:02d}.txt")
            curr_file.write('')
    assert len(directory.listdir()) == 11
    return directory


file_extractor_ans = [(3, r'[7-9]\.txt'), (1, '01.txt'), (10, None)]


@pytest.mark.parametrize('expected_len,pattern', file_extractor_ans)
def test_file_extractor(expected_len, pattern, test_dir):
    extractor = FileExtractor(directory=test_dir, pattern=pattern)
    extractor.setup()
    extract_generator = extractor.extract()
    files = list(extract_generator)
    assert len(files) == expected_len


@pytest.mark.parametrize('expected_len,pattern', file_extractor_ans + [(5, r'1-\d+.txt')])
def test_recursive_file_extractor(expected_len, pattern, test_dir):
    """Test the recursive flag on the file extractor."""
    extractor = FileExtractor(directory=test_dir, pattern=pattern, recursive=True)
    extractor.setup()
    extract_generator = extractor.extract()
    files = list(extract_generator)
    assert len(files) == expected_len * 2


# CSV Extractor


@pytest.fixture
def temp_csv_path(tmpdir):
    csv_path = join(tmpdir, "test.csv")
    N = 3
    with open(csv_path, "w") as f:
        writer = csv.writer(f, delimiter=",")
        for i in range(N):
            writer.writerow([f"key{i}", f"value{i}"])

    return csv_path


def test_csv_extractor(temp_csv_path):
    extractor = CSVExtractor(full_path=temp_csv_path)
    extractor.setup()
    for i, row in enumerate(extractor.extract()):
        assert row == [f"key{i}", f"value{i}"]

    assert extractor.length() == i + 1


# S3 Extractor


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


def test_s3_extractor(mock_s3_bucket, bucket_name):
    extractor = S3FileExtractor(bucket=bucket_name)
    file_names, file_contents = mock_s3_bucket
    for i, (key, contents) in enumerate(extractor.extract()):
        assert key == file_names[i]
        assert contents == file_contents[i]
