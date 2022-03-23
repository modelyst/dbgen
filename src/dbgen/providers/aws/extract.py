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

import re
from typing import Any
from typing import Generator as GenType
from typing import List, Optional, Tuple

import boto3

from dbgen import Extract


class S3FileNameExtractor(Extract[str]):
    bucket: str
    profile: Optional[str]
    prefix: str = ''
    pattern: str = '.*'
    outputs: List[str] = ['s3_key']

    def extract(self) -> GenType[str, None, None]:
        """Return the files in a directory on s3 bucket that match pattern."""
        session = boto3.Session(profile_name=self.profile)
        s3 = session.client("s3")
        regex = re.compile(self.pattern, re.MULTILINE)

        response = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        assert "Contents" in response, response
        if not response or response["KeyCount"] == 0:
            return

        def process_keys(response: dict) -> Tuple[List[str], Any]:
            return (
                [content["Key"] for content in response["Contents"] if regex.search(content["Key"])],
                response.get("NextContinuationToken"),
            )

        vals, token = process_keys(response)
        yield from vals
        count = 0
        while token:
            response = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix, ContinuationToken=token)
            if not response["IsTruncated"]:
                break

            keys, token = process_keys(response)
            yield from keys
            count += response["KeyCount"]


class S3FileExtractor(S3FileNameExtractor):
    """Returns both the key and contents of the file on s3"""

    outputs: List[str] = ['s3_key', 'contents']

    def extract(self) -> GenType[Tuple[str, str], None, None]:  # type: ignore
        session = boto3.Session(profile_name=self.profile)
        s3 = session.resource("s3")
        key_gen = super().extract()
        bucket = s3.Bucket(self.bucket)
        for key in key_gen:
            obj = bucket.Object(key=key)
            body = obj.get()["Body"].read().decode("utf-8")
            yield key, body
