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

from itertools import chain
from json import dumps
from pathlib import Path
from typing import List
from uuid import UUID

import typer
from rich.console import Console
from rich.table import Table
from sqlalchemy import update
from sqlmodel import Session, select

import dbgen.cli.styles as styles
from dbgen.cli.options import (
    chdir_option,
    config_option,
    model_arg_option,
    model_string_option,
    verbose_option,
)
from dbgen.cli.utils import state, test_connection, validate_model_str
from dbgen.configuration import get_connections, update_config
from dbgen.core.metadata import ModelEntity

model_app = typer.Typer(name='model', no_args_is_help=True)


@model_app.command('list')
def list_models(config_file: Path = config_option, tags: List[str] = typer.Option(None, '-t')):
    """List the models in the metadatabase."""
    # Notify of config file
    _, meta_conn = get_connections()
    test_connection(meta_conn)
    meta_engine = meta_conn.get_engine()
    tags = tags or []
    statement = select(  # type: ignore
        ModelEntity.id,
        ModelEntity.name,
        ModelEntity.created_at,
        ModelEntity.last_run,
        ModelEntity.tags,
    )  # type: ignore
    if tags:
        statement = statement.where(ModelEntity.tags.op('&&')(tags))  # type: ignore
    columns = ['id', 'name', 'created_at', 'last_run', 'tags']
    table = Table(
        *columns,
        show_lines=True,
        highlight=True,
        border_style='magenta',
    )
    with Session(meta_engine) as session:
        result = session.exec(statement)
        for model_id, model_name, created_at, last_run, tags in result:
            table.add_row(*map(str, (model_id, model_name, created_at, last_run, tags)))
    console = Console()
    console.print(table)


@model_app.command('tag')
def tag(model_id: UUID, tags: List[str], config_file: Path = config_option):
    # Notify of config file
    _, meta_conn = get_connections()
    test_connection(meta_conn)
    meta_engine = meta_conn.get_engine()

    with Session(meta_engine) as session:
        existing_tags = session.exec(select(ModelEntity.tags).where(ModelEntity.id == model_id)).one_or_none()
        if existing_tags is None:
            raise typer.BadParameter(f"Invalid model_id, no model with ID {model_id}")
        new_tags = set(chain(existing_tags, tags))
        session.execute(update(ModelEntity).values(tags=new_tags).where(ModelEntity.id == model_id))

        session.commit()


@model_app.command('serialize')
def model_serialize(
    model_str: str = model_arg_option,
    out_file: Path = typer.Option(
        None, '-o', '--out', help='Path to write the serialized model to in json format'
    ),
    config_file: Path = config_option,
):
    model = validate_model_str(model_str)

    # Notify of config file
    _, meta_conn = get_connections()
    test_connection(meta_conn)
    meta_engine = meta_conn.get_engine()
    with Session(meta_engine) as session:
        model_row = model._get_model_row()
        # Check for existing row and if found grab its created_at
        created_at = session.exec(
            select(ModelEntity.created_at).where(ModelEntity.id == model.uuid)
        ).one_or_none()
        if created_at is None:
            session.merge(model_row)
            session.commit()
            styles.good_typer_print(f"Loaded model {model.name!r} into the database with ID {model.uuid}")
        else:
            styles.good_typer_print(f"Model {model.name!r} already existed with ID {model.uuid}")
    if out_file:
        out_file.write_text(dumps(model_row.graph_json))
        styles.good_typer_print(f"Wrote serialized graph to {out_file}")


@model_app.command('export')
def model_export(
    model_id: UUID,
    out_file: Path = typer.Option(
        'model.json', '-o', '--out', help='Path to write the serialized model to in json format'
    ),
    config_file: Path = config_option,
):

    # Notify of config file
    _, meta_conn = get_connections()
    test_connection(meta_conn)
    meta_engine = meta_conn.get_engine()

    with Session(meta_engine) as session:
        # Check for existing row and if found grab its created_at
        graph_json = session.exec(
            select(ModelEntity.graph_json).where(ModelEntity.id == model_id)
        ).one_or_none()
        if not graph_json:
            raise ValueError(f"Invalid model_id: No model found with model_id {model_id}")

    out_file.write_text(dumps(graph_json))
    styles.good_typer_print(f"Wrote serialized graph to {out_file}")


@model_app.command('validate')
def validate(
    model_str: str = model_string_option,
    config_file: Path = config_option,
    _verbose: bool = verbose_option(),
    _chdir: Path = chdir_option,
):
    """Quick utility method for quickly validating a model will compile without any need for database connections."""
    # Start connection from config
    config = update_config(config_file)
    # Use config model_str if none is provided
    if model_str is None:
        model_str = config.model_str
    model = validate_model_str(model_str)
    styles.good_typer_print(f"Model(name={model.name!r}) was successfully validated")
    styles.good_typer_print(
        f'Model contains {len(model.etl_steps)} ETLStep(s) and {len(model.registry.metadata.tables)} two entities.'
    )
    if state['verbose']:
        styles.good_typer_print(f'Model contains {len(model.etl_steps)} ETLStep(s).')
