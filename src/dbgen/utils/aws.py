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

# Use this code snippet in your app. If you need more information about
# configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/

from json import loads

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore


def get_secret(
    secret_id: str,
    region_name: str,
    profile_name: str = None,
) -> dict:
    """Get secret from aws secret manager service and return as dict.

    Args:
        secret_id (str): secret id on AWS
        region_name (str): AWS region of secret
        profile_name (str, optional): AWS profile to query without. Defaults to None.

    Returns:
        dict: Secret stored on AWS secret manager as dict

    """
    # Create a Secrets Manager client
    session = boto3.session.Session(profile_name=profile_name)
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # In this sample we only handle the specific exceptions for the
    # 'GetSecretValue' API. See
    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # Secrets Manager can't decrypt the protected secret text using the
            # provided KMS key. Deal with the exception here, and/or rethrow at
            # your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred on the server side. Deal with the exception
            # here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter. Deal with the
            # exception here, and/or rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a parameter value that is not valid for the current
            # state of the resource. Deal with the exception here, and/or
            # rethrow at your discretion.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # We can't find the resource that you asked for. Deal with the
            # exception here, and/or rethrow at your discretion.
            raise e
        raise e
    # Your code goes here.
    return loads(get_secret_value_response["SecretString"])
