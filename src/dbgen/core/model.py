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
from collections.abc import Iterable
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import sqlalchemy
from networkx import DiGraph
from pydantic import Field, validator
from sqlalchemy import MetaData, inspect, text
from sqlalchemy.future import Engine
from sqlalchemy.orm import registry as sa_registry

from dbgen.core.base import Base
from dbgen.core.entity import Entity
from dbgen.core.generator import Generator
from dbgen.core.metadata import meta_registry
from dbgen.utils.graphs import topsort_with_dict

if TYPE_CHECKING:
    from dbgen.core.run import RunConfig


class Model(Base):
    name: str
    generators: List[Generator] = Field(default_factory=list)
    registry: sa_registry = Field(default_factory=lambda: Entity._sa_registry)
    meta_registry: sa_registry = Field(default_factory=lambda: meta_registry)
    _hashinclude_ = {"name", "generators"}

    class Config:
        """Pydantic Config"""

        arbitrary_types_allowed = True

    @validator("generators")
    def unique_generator_names(cls, value: List[Generator]) -> List[Generator]:
        name_count = Counter([x.name for x in value])
        dup_names = {name for name, count in name_count.items() if count > 1}
        if dup_names:
            raise ValueError(f"Found duplicate generator names, all names must be unique: {dup_names}")
        return value

    @validator("generators")
    def sort_generators(cls, value: List[Generator]) -> List[Generator]:
        return value

    def gens(self) -> Dict[str, Generator]:
        return {gen.name: gen for gen in self.generators}

    def add_gen(self, generator: Generator):
        if generator.name not in self.gens():
            self.generators.append(generator)
            return
        raise ValueError(
            f"Generator named {generator.name} already in model please use a new name:\n{self.gens().keys()}"
        )

    def validate_marker(self, marker: str) -> bool:
        """Compare a marker to the models generators to see if it is valid."""
        valid_marker = False
        for gen in self.generators:
            if marker == gen.name or marker in gen.tags:
                valid_marker = True
        return valid_marker

    def _generator_graph(self) -> "DiGraph":
        from networkx import DiGraph

        graph = DiGraph()

        # Add edges for every Arg in graph
        edges: List[Tuple[str, str]] = []
        for source_name, source in self.gens().items():
            for target_name, target in self.gens().items():
                if source_name != target_name:
                    if source._get_dependency().test(target._get_dependency()):
                        edges.append((target_name, source_name))

        for gen in self.gens().values():
            graph.add_node(gen.name, data=gen)
        graph.add_edges_from(edges)
        return graph

    def _sort_graph(self) -> List[Generator]:
        graph = self._generator_graph()
        sorted_node_ids = topsort_with_dict(graph)
        sorted_nodes = [graph.nodes[key]["data"] for key in sorted_node_ids]
        return sorted_nodes

    def run(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_config: "RunConfig" = None,
        nuke: bool = False,
        rerun_failed: bool = False,
    ):
        from dbgen.core.run import ModelRun

        return ModelRun(model=self).execute(
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
        metadata.drop_all(engine)
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