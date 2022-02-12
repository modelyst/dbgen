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

from collections import Counter
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple
from uuid import UUID

import sqlalchemy
from networkx import DiGraph
from pydantic import Field, PrivateAttr, validator
from sqlalchemy import MetaData, inspect, text
from sqlalchemy.future import Engine
from sqlalchemy.orm import registry as sa_registry

from dbgen.core.base import Base
from dbgen.core.context import ModelContext
from dbgen.core.entity import BaseEntity
from dbgen.core.etl_step import ETLStep
from dbgen.core.metadata import ModelEntity, RunEntity, meta_registry
from dbgen.exceptions import DatabaseError
from dbgen.utils.graphs import serialize_graph, topsort_with_dict

if TYPE_CHECKING:
    from dbgen.core.run import RunConfig  # pragma: no cover


class Model(Base):
    name: str
    etl_steps: List[ETLStep] = Field(default_factory=list)
    registry: sa_registry = Field(default_factory=lambda: BaseEntity._sa_registry)
    meta_registry: sa_registry = Field(default_factory=lambda: meta_registry)
    _context: ModelContext = PrivateAttr(None)
    _hashinclude_ = {"name", "etl_steps"}

    class Config:
        """Pydantic Config"""

        arbitrary_types_allowed = True

    def __enter__(self):
        self._context = ModelContext(context_dict={'model': self})
        return self._context.__enter__()['model']

    def __exit__(self, *args):
        self._context.__exit__(*args)
        self.validate(self)
        del self._context

    @validator("etl_steps")
    def unique_etl_step_names(cls, value: List[ETLStep]) -> List[ETLStep]:
        name_count = Counter([x.name for x in value])
        dup_names = {name for name, count in name_count.items() if count > 1}
        if dup_names:
            raise ValueError(f"Found duplicate etl_step names, all names must be unique: {dup_names}")
        return value

    @validator("etl_steps")
    def sort_etl_steps(cls, value: List[ETLStep]) -> List[ETLStep]:
        return value

    def etl_steps_dict(self) -> Dict[str, ETLStep]:
        return {etl_step.name: etl_step for etl_step in self.etl_steps}

    def add_etl_step(self, etl_step: ETLStep):
        if etl_step.name not in self.etl_steps_dict():
            self.etl_steps.append(etl_step)
            return
        raise ValueError(
            f"ETLStep named {etl_step.name} already in model please use a new name:\n{self.etl_steps_dict().keys()}"
        )

    def validate_marker(self, marker: str) -> bool:
        """Compare a marker to the models etl_steps to see if it is valid."""
        valid_marker = False
        for etl_step in self.etl_steps:
            if marker == etl_step.name or marker in etl_step.tags:
                valid_marker = True
        return valid_marker

    def _etl_step_graph(self) -> "DiGraph":
        from networkx import DiGraph

        graph = DiGraph()

        # Add edges for every Arg in graph
        edges: List[Tuple[str, str]] = []
        for source_name, source in self.etl_steps_dict().items():
            for target_name, target in self.etl_steps_dict().items():
                if source_name != target_name:
                    if source._get_dependency().test(target._get_dependency()):
                        edges.append((target_name, source_name))

        for etl_step in self.etl_steps_dict().values():
            graph.add_node(etl_step.name, id=etl_step.uuid, **etl_step.dict(include={'name', 'dependency'}))
        graph.add_edges_from(edges)
        return graph

    def _sort_graph(self) -> List[ETLStep]:
        graph = self._etl_step_graph()
        etl_steps = self.etl_steps_dict()
        sorted_node_ids = topsort_with_dict(graph)
        sorted_nodes = [etl_steps[key] for key in sorted_node_ids]
        return sorted_nodes

    def run(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_config: "RunConfig" = None,
        nuke: bool = False,
        rerun_failed: bool = False,
        remote: bool = True,
    ) -> RunEntity:
        from dbgen.core.run import ModelRun, RemoteModelRun

        Runner = RemoteModelRun if remote else ModelRun
        return Runner(model=self).execute(
            main_engine=main_engine,
            meta_engine=meta_engine,
            run_config=run_config,
            nuke=nuke,
            rerun_failed=rerun_failed,
        )

    def sync(self, main_engine: Engine, meta_engine: Engine, nuke: bool = False) -> None:
        """Syncs the state of the models registry with the database."""
        # Inspect the schemas and make sure they all exist
        for engine, metadata in (
            (main_engine, self.registry.metadata),
            (meta_engine, self.meta_registry.metadata),
        ):
            inspector = inspect(engine)
            expected_schemas = {x.schema for x in metadata.tables.values() if x.schema}
            current_schemas = inspector.get_schema_names()

            # Nuking drops all tables in the schemas of the model
            if nuke:
                self.nuke(engine, metadata=metadata, schemas=expected_schemas)

            missing_schema = {x for x in expected_schemas if x not in current_schemas}
            for schema in missing_schema:
                self._logger.debug(f"Missing schema {schema}, creating now")
                with engine.begin() as conn:
                    conn.execute(text(f"CREATE SCHEMA {schema}"))

        # Create all tables
        self.registry.metadata.create_all(main_engine)

        # Create meta schema
        self.meta_registry.metadata.create_all(meta_engine)

    def nuke(
        self,
        engine: Engine,
        metadata: MetaData,
        schemas: Optional[Iterable[Optional[str]]] = None,
        nuke_all: bool = False,
    ) -> None:
        # Validate and set the schema
        schemas = schemas or []
        if not schemas:
            # Nuke all grabs all schemas in the database
            # Otherwise we use None and the default schema in the engine will be dropped
            if nuke_all:
                schemas = inspect(engine).get_schema_names()
            else:
                schemas = [None]

        assert isinstance(schemas, Iterable), f"schemas must be iterable: {schemas}"
        # Drop the expected tables
        try:
            metadata.drop_all(engine)
        except sqlalchemy.exc.InternalError as exc:
            raise DatabaseError("Error occurred during nuking. Please drop the schema manually...") from exc
        # Iterate through the schemas this metadata describes and drop all the tables within them
        for schema in schemas:
            self._logger.info(f"Nuking the schema={schema!r}...")
            metadata = MetaData()
            metadata.reflect(engine, schema=schema)
            try:
                metadata.drop_all(engine)
            except sqlalchemy.exc.InternalError:
                with engine.begin() as conn:
                    conn.execute(text(f'DROP SCHEMA {schema} CASCADE'))
                # raise ValueError(
                #     f"Nuking has failed! Dependent objects exist that SQL alchemy cannot successfully drop from the schema {schema!r}.\n"
                #     f"This often occurs when Views exist in the schema. Please drop these views manually by running 'DROP SCHEMA {schema} CASCADE'"
                # )
            self._logger.info(f"Schema {schema!r} nuked.")

    def _get_model_row(self):
        graph = self._etl_step_graph()

        def default(obj):
            if isinstance(obj, set):
                return list(obj)
            elif isinstance(obj, UUID):
                return str(obj)
            raise TypeError(f"Unknown type: {type(obj)}")

        metadata = {'name': self.name, 'id': self.uuid}
        graph_json = serialize_graph(graph, lambda x: x, metadata=metadata)
        # Assemble stringified dependency fields as we can't store sets in postgres easily
        return ModelEntity(
            id=self.uuid,
            name=self.name,
            etl_steps=[x._get_etl_step_row() for x in self.etl_steps],
            graph_json=graph_json,
        )
