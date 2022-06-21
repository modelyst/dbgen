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
from json import loads
from typing import TYPE_CHECKING, Dict, List, Tuple
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
from dbgen.exceptions import ModelRunError
from dbgen.utils.graphs import serialize_graph, topsort_with_dict

if TYPE_CHECKING:
    from dbgen.core.run.utilities import RunConfig  # pragma: no cover


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
        del self._context
        self.validate(self)

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
            graph.add_node(etl_step.name, id=etl_step.hash, **etl_step.dict(include={'name', 'dependency'}))
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
        build: bool = False,
        rerun_failed: bool = False,
        remote: bool = True,
        run_async: bool = True,
    ) -> RunEntity:
        from dbgen.core.run.model_run import ModelRun

        # Check for empty model
        if not self.etl_steps:
            raise ModelRunError(
                f'Model {self.name} has no ETLSteps to run. Did you make sure to instantiate your ETLSteps within the model\'s context?'
            )

        return ModelRun(model=self).execute(
            main_engine=main_engine,
            meta_engine=meta_engine,
            run_config=run_config,
            build=build,
            run_async=run_async,
            remote=remote,
            rerun_failed=rerun_failed,
        )

    def sync(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        build: bool = False,
        meta_only: bool = False,
        create: bool = True,
    ) -> None:
        """Syncs the state of the models registry with the database."""
        if not build and not create:
            raise ValueError("Not building and not creating, sync is doing nothing...")
        if not meta_only:
            if build:
                self.drop_metadata(main_engine, self.registry.metadata)
            if create:
                self.create_metadata(main_engine, self.registry.metadata)
        if build:
            self.drop_metadata(meta_engine, self.meta_registry.metadata)
        if create:
            self.create_metadata(meta_engine, self.meta_registry.metadata)

    def drop_metadata(self, engine: Engine, metadata: MetaData):
        try:
            metadata.drop_all(engine)
        except sqlalchemy.exc.InternalError as exc:
            raise ValueError("Error occurred during rebuilding. Please drop the schema manually...") from exc

    def create_metadata(self, engine: Engine, metadata: MetaData):
        inspector = inspect(engine)
        expected_schemas = {x.schema for x in metadata.tables.values() if x.schema}
        current_schemas = inspector.get_schema_names()
        missing_schema = {x for x in expected_schemas if x not in current_schemas}
        for schema in missing_schema:
            self._logger.debug(f"Missing schema {schema}, creating now")
            with engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA {schema}"))
        # Create meta schema
        try:
            metadata.create_all(engine)
        except sqlalchemy.exc.InternalError as exc:
            raise ValueError("Error occurred during database creation") from exc

    def _get_model_row(self):
        graph = self._etl_step_graph()

        def default(obj):
            if isinstance(obj, set):
                return list(obj)
            elif isinstance(obj, UUID):
                return str(obj)
            return obj

        metadata = {'name': self.name, 'id': self.uuid}
        graph_json = serialize_graph(graph, default, metadata=metadata)
        # Assemble stringified dependency fields as we can't store sets in postgres easily
        return ModelEntity(
            id=self.uuid,
            name=self.name,
            etl_steps=[x._get_etl_step_row() for x in self.etl_steps],
            graph_json=loads(graph_json),
        )
