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

import os
from json import loads
from pathlib import Path
from typing import List, Optional

import typer
from pydantic import BaseSettings, Field
from rich.prompt import Confirm

import docker
from dbgen.providers.aws.ecs import AWSConfiguration, ECSTask, build_and_push_to_ecr, get_aws_settings
from dbgen.providers.aws.logs import follow_logs
from dbgen.utils.log import logging_console

default_dockerfile = Path(__file__).parent.parent / 'docker' / 'Dockerfile'
deploy_app = typer.Typer()
settings_option = typer.Option(os.environ.get('DBGEN_CONFIG', '.env'), "--settings")

# Settings and configuration
class DeploymentSettings(BaseSettings):
    """Settings for the deployment of a model to ECS"""

    task_name: str = "lab-pipeline-task"
    cluster: str = "test-pipeline-staging-cluster"

    subnet: List[str] = Field(default_factory=list)
    command: str = "run"
    environment_path: Optional[Path]

    @property
    def environment(self) -> List[dict]:
        if self.environment_path is None or not self.environment_path.exists():
            return []
        return loads(self.environment_path.read_text())

    class Config:
        """Pydantic Config"""

        _env_file = os.environ.get("DBGEN_CONFIG", ".env")
        env_prefix = "DBGEN_"


@deploy_app.command('build')
def build(
    path: Path = Path.cwd(),
    dockerfile: str = str(default_dockerfile),
    model_folder: str = typer.Argument(..., help='Path to model to copy into image.'),
    model_name: str = typer.Option(None, '--model-name', help='Path to model to copy into image.'),
    tags: List[str] = typer.Option(None, '--tag', help='Path to model to copy into image.'),
    mounts: List[str] = typer.Option(None, '--mounts', help='Path to model to copy into image.'),
    env_file: Path = typer.Option(None, help='Path to model env file.'),
):
    client = docker.from_env()

    mounts = mounts or mounts

    buildargs = {}

    if env_file:
        buildargs['ENV_FILE'] = str(env_file)

    if model_folder:
        buildargs['MODEL_FOLDER'] = str(model_folder)

    buildargs['MODEL_NAME'] = model_name or model_folder

    tags = tags or ['dbgen_model:latest']
    for tag in tags:
        _, status = client.images.build(
            path=str(path),
            dockerfile=dockerfile,
            tag=tag,
            buildargs=buildargs,
        )
        for line in status:
            text = line.get('stream', '').strip()
            if text:
                print(text)


@deploy_app.command('ecr-push')
def ecr_push(
    path: Path = Path.cwd(),
    image: str = typer.Argument(..., help='Local image to push to ECR.'),
    repo_name: Optional[str] = typer.Option(None, '--repo', help='ECR Repo name to push the image to.'),
    tags: List[str] = typer.Option(None, '--tag', help='Path to model to copy into image.'),
    push: bool = typer.Option(False, '--push', help='Path to model to copy into image.'),
):
    """Build and push a dbgen docker image to the ECR repository."""
    aws_config = AWSConfiguration()
    session = aws_config.get_session()
    repo_name = repo_name or aws_config.aws_ecr_repo
    if repo_name is None:
        raise ValueError(f"No repo provided in CLI or environment.")
    tags = list(tags) or []
    with logging_console.status('Building and pushing image') as status:
        build_and_push_to_ecr(str(path), image, repo_name, session, tags, push=push, status_update=status)


@deploy_app.command("run-task")
def run_task(
    task: Optional[str] = typer.Option(None, "--task", help="Task Definition Name"),
    cluster: Optional[str] = typer.Option(None, "--cluster", help="Cluster Name"),
    setting_path: Optional[Path] = settings_option,
    environment_path: Optional[Path] = typer.Option(
        None, "--env", "-e", help='Read in a json environment file and override the container.'
    ),
    memory: Optional[int] = typer.Option(512, help='Override memory allocation for task.'),
    cpu: Optional[int] = typer.Option(256, help='Override cpu allocation for task.'),
    command: Optional[str] = "run",
    subnets: Optional[List[str]] = typer.Option(None, '--subnet', help='Set the default'),
    vpc_id: Optional[str] = typer.Option(
        None, help="The AWS VPC id used to get the subnets when none are provided."
    ),
    follow: Optional[bool] = typer.Option(True, help="Follow the logs of the task once submitted."),
):
    """Run a DBgen ECS task."""

    # Initialize the settings
    deploy_settings = DeploymentSettings(_env_file=setting_path)
    aws_settings = get_aws_settings(setting_path)

    # Initialize Boto3
    session = aws_settings.get_session()
    ecs = session.client("ecs")
    logs = session.client("logs")

    # Override settings with CLI
    if task:
        deploy_settings.task_name = task
    if cluster:
        deploy_settings.cluster = cluster
    if environment_path:
        deploy_settings.environment_path = environment_path
    if subnets:
        deploy_settings.subnet = subnets

    # Warn user if both --subnet and --vpc-id are set
    if subnets and vpc_id:
        logging_console.print('Both --subnet have been set alongside --vpc, ignoring --vpc', style='dim red')
    # If no subnets are found use default subnets
    if not deploy_settings.subnet:
        deploy_settings.subnet = aws_settings.get_default_subnets(vpc_id=vpc_id)
        logging_console.print(f'Using default subnets: {deploy_settings.subnet}')

    networkConfiguration = {
        "awsvpcConfiguration": {
            "subnets": deploy_settings.subnet,
            "assignPublicIp": "ENABLED",
        }
    }
    command_list = command.split(" ") if command else deploy_settings.command
    environment = deploy_settings.environment
    overrides = [
        {
            "name": 'dbgen-container',
            "command": command_list or ["run"],
            "environment": environment,
            "memory": memory,
            "cpu": cpu,
        }
    ]
    inputs = dict(
        cluster=deploy_settings.cluster,
        launchType="FARGATE",
        networkConfiguration=networkConfiguration,
        overrides={"containerOverrides": overrides, "memory": str(memory), "cpu": str(cpu)},
        count=1,
        taskDefinition=deploy_settings.task_name,
    )
    logging_console.print(inputs)
    if not Confirm.ask(
        f"Submit the task above to cluster: {deploy_settings.cluster}?", console=logging_console
    ):
        raise typer.Exit(0)
    out = ecs.run_task(**inputs)
    task_arn = out["tasks"][0]["taskArn"]
    task_id = task_arn.split(f"task/{deploy_settings.cluster}/")[-1]
    logging_console.print(f"Submitted Task {task_id!r}")

    ecs_task = ECSTask(task_id=task_id, task_name=task, cluster_name=deploy_settings.cluster)
    # Wait for container and log stream provision
    with logging_console.status(f'Waiting for Task {ecs_task.task_id} to be provisioned'):
        ecs_task.wait_for_provision(ecs)
    with logging_console.status(f"Waiting for Log Stream {ecs_task.get_log_stream()} to be created"):
        ecs_task.wait_for_log_stream_creation(logs)
    # If not following exit
    if not follow:
        logging_console.print(f'Follow the logs by running the command `dbgen deploy logs --task {task_id}')
        raise typer.Exit(0)

    # Follow the logs
    # Close
    logging_console.print(f"Following Task ID Logs {task_id !r}")
    logging_console.rule(style="magenta")
    logging_console.print("Beginning of logs")
    logging_console.rule(style="magenta")
    for event in follow_logs(logs, ecs, ecs_task):
        logging_console.print(event["message"])

    task_details = ecs_task.get_task_details(ecs)
    exit_code = task_details.exitCode
    # Close
    logging_console.rule(style="magenta")
    logging_console.print("End of logs")
    logging_console.print(f"Task state updated to {task_details.lastStatus}")
    logging_console.rule(style="magenta")
    if exit_code is not None:
        if exit_code == 0:
            logging_console.rule(style="green")
            logging_console.print(
                f"Task Successfully completed with exit code {exit_code}.",
                style="green",
            )
            logging_console.print("Shutting down", style="green")
            logging_console.rule(style="green")
        else:
            logging_console.rule(style="red")
            logging_console.print(f"Task stopped with exit code {exit_code}.", style="red")
            logging_console.print("Shutting down", style="red")
            logging_console.rule(style="red")
        raise typer.Exit(exit_code)


@deploy_app.command("logs")
def get_logs(
    task_id: str,
    task_name: Optional[str] = typer.Option(None, '--task'),
    setting_path: Optional[Path] = settings_option,
    interval: int = typer.Option(5, '--interval', help='Time between pinging AWS.'),
):
    """Follow the logs of a given task."""
    deploy_settings = DeploymentSettings(_env_file=setting_path)
    aws_settings = get_aws_settings(setting_path)
    session = aws_settings.get_session()

    if task_name:
        deploy_settings.task_name = task_name
    logs = session.client('logs')
    ecs = session.client('ecs')
    ecs_task = ECSTask(
        task_id=task_id,
        task_name=deploy_settings.task_name,
        cluster_name=deploy_settings.cluster,
        interval=interval,
    )
    for event in follow_logs(logs, ecs, ecs_task):
        logging_console.print(event["message"])


@deploy_app.command("list")
def list_tasks(
    setting_path: Optional[Path] = settings_option,
):
    """Follow the logs of a given task."""
    # Start boto3 ecs session
    aws_settings = get_aws_settings(setting_path)
    session = aws_settings.get_session()
    ecs = session.client('ecs')
    # Query the task definitions
    with logging_console.status('Getting task definitions'):
        output = ecs.list_task_definitions()
    # parse task names from arns
    tasks = [arn.split('task-definition/')[-1].split(':')[0] for arn in output['taskDefinitionArns']]
    # Report back to user
    logging_console.print(f'Found {len(tasks)} Task(s)')
    for task in tasks:
        logging_console.print(f'- {task!r}')
