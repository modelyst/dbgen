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

import base64
import logging
import os
import time
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple

import boto3
from pydantic import BaseModel, BaseSettings

import docker

logger = logging.getLogger(__name__)
docker_client = docker.from_env()


class AWSConfiguration(BaseSettings):
    aws_region: str = 'us-east-2'
    aws_profile: Optional[str]
    aws_access_key: Optional[str]
    aws_secret_access_key: Optional[str]
    aws_ecr_repo: Optional[str]

    _session: Optional[boto3.Session] = None

    class Config:
        """Pydantic configuration"""

        env_file = os.environ.get("DBGEN_CONFIG", ".env")
        env_prefix = "DBGEN_"
        underscore_attrs_are_private = True

    def get_session(self) -> boto3.Session:
        """
        Get a boto3 access key from the credentials stored in the aws configuration
        """
        # TODO Check if memoization is okay here
        # memoize the session as the configuration shouldn't be changing...
        if self._session is not None:
            return self._session
        if self.aws_profile:
            return boto3.Session(region_name=self.aws_region, profile_name=self.aws_profile)
        elif self.aws_secret_access_key and self.aws_access_key:
            return boto3.Session(
                region_name=self.aws_region,
                aws_access_key=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        raise ValueError(
            'Cannot retrieve a boto3 session neither DBGEN_AWS_PROFILE or (DBGEN_AWS_ACCESS_KEY, DBGEN_AWS_SECRET_ACCESS_KEY) set'
        )

    def get_default_subnets(self, vpc_id: Optional[str] = None) -> List[str]:
        """Get the default subnet arguments when no subnet is provided by user."""
        ec2 = self.get_session().client('ec2')
        subnets = ec2.describe_subnets()['Subnets']
        return [
            x['SubnetId']
            for x in filter(
                lambda x: (vpc_id is None and x['DefaultForAz']) or x.get('VpcId') == vpc_id, subnets
            )
        ]


def get_aws_settings(settings_path: Optional[Path] = None):
    """Get the settings object from either a path or default locaiton."""
    return AWSConfiguration(_env_file=settings_path or os.environ.get('DBGEN_CONFIG', '.env'))


def get_account_id(session: boto3.Session):
    return session.client('sts').get_caller_identity().get('Account')


def authenticate_ecr(ecr_client):
    token = ecr_client.get_authorization_token()
    username, password = (
        base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
    )
    registry = token['authorizationData'][0]['proxyEndpoint']

    docker_client.login(username=username, password=password, registry=registry)


def get_repo_details(repo_name: str, ecr_client):
    repo = ecr_client.describe_repositories(repositoryNames=[repo_name])
    return repo['repositories'][0]


def build_and_push_to_ecr(
    path: str,
    image: str,
    repo_name: str,
    session: boto3.Session,
    tags: List[str],
    push: bool = False,
    status_update=None,
):
    ecr_client = session.client('ecr')
    authenticate_ecr(ecr_client)
    repo_details = get_repo_details(repo_name, ecr_client)
    tags.append('latest')
    full_tags = [f"{repo_details['repositoryUri']}:{tag}" for tag in tags]
    token = ecr_client.get_authorization_token()
    username, password = (
        base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
    )
    registry = token['authorizationData'][0]['proxyEndpoint']

    docker_client.login(username=username, password=password, registry=registry)
    for full_tag, simple_tag in zip(full_tags, tags):
        if status_update:
            status_update.update(f'Building image: {full_tag}')
        docker_image, status = docker_client.images.build(path=path, dockerfile=image, tag=full_tag)
        logger.info(f'Built Image {full_tag}')
        if status_update:
            status_update.update(f'Pushing image: {full_tag}')
        docker_client.images.push(repo_details['repositoryUri'], tag=simple_tag)
        logger.info(f'Pushed Image {full_tag}')


class TaskStatus(str, Enum):
    PROVISIONING = 'PROVISIONING'
    DEPROVISIONING = 'DEPROVISIONING'
    STOPPED = 'STOPPED'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'


class TaskDetails(BaseModel):
    task_id: str
    taskArn: str
    exitCode: Optional[int]
    lastStatus: TaskStatus


class ECSTask(BaseModel):
    task_id: str
    task_name: str
    cluster_name: str
    interval: int = 5

    def get_task_details(self, client) -> TaskDetails:
        output = client.describe_tasks(tasks=(self.task_id,), cluster=self.cluster_name)

        task_details = TaskDetails(
            task_id=self.task_id,
            taskArn=output["tasks"][0]["taskArn"],
            lastStatus=output["tasks"][0]["lastStatus"],
            exitCode=output["tasks"][0]["containers"][0].get("exitCode"),
        )
        return task_details

    def wait_for_provision(self, client) -> None:
        while True:
            task_details = self.get_task_details(client)
            if task_details.lastStatus in TaskStatus.PROVISIONING:
                time.sleep(self.interval)
            else:
                break

    def check_finished_task(self, client) -> Tuple[Optional[str], Optional[int]]:
        task_details = self.get_task_details(client)
        if task_details.lastStatus in (TaskStatus.STOPPED, TaskStatus.DEPROVISIONING):
            return task_details.lastStatus, task_details.exitCode
        return None, None

    def wait_for_log_stream_creation(self, log_client) -> None:
        while True:
            log_stream = log_client.describe_log_streams(
                logGroupName=self.task_name, logStreamNamePrefix=self.get_log_stream()
            )
            streams = log_stream.get("logStreams", [])
            if streams:
                break
            time.sleep(self.interval)

    def get_log_stream(self) -> str:
        return f'{self.task_name}/dbgen-container/{self.task_id}'
