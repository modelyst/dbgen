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

from functools import reduce
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

from pydantic import Field
from sqlalchemy.future import Engine

from dbgen.core.args import Arg
from dbgen.core.base import Base
from dbgen.core.dependency import Dependency
from dbgen.core.extract import Extract
from dbgen.core.load import Load
from dbgen.core.metadata import GeneratorEntity
from dbgen.core.query import BaseQuery
from dbgen.core.transforms import PyBlock
from dbgen.exceptions import DBgenMissingInfo
from dbgen.utils.graphs import topsort_with_dict

if TYPE_CHECKING:
    from networkx import DiGraph

    from dbgen.core.computational_node import ComputationalNode

list_field = Field(default_factory=lambda: [])


class Generator(Base):
    name: str
    description: str = "<no description>"
    extract: Union[BaseQuery, Extract] = Field(default_factory=Extract)
    transforms: List[PyBlock] = list_field
    loads: List[Load] = list_field
    tags: List[str] = list_field
    batch_size: Optional[int] = None
    additional_dependencies: Optional[Dependency] = None
    _graph: Optional["DiGraph"]

    def __str__(self) -> str:
        return f"Gen<{self.name}>"

    def _computational_graph(self) -> "DiGraph":
        from networkx import DiGraph

        nodes: Dict[str, "ComputationalNode"] = {self.extract.hash: self.extract}
        # Add transforms and loads
        nodes.update({transform.hash: transform for transform in self.transforms})
        nodes.update({load.hash: load for load in self.loads})
        # Add edges for every Arg in graph
        edges: List[Tuple[str, str]] = []
        for node_id, node in nodes.items():
            for key, arg in node.inputs.items():
                if isinstance(arg, Arg):
                    if arg.key not in nodes:
                        raise DBgenMissingInfo(
                            f"Argument {key} of {node} refers to an object with a hash key {arg.key} asking for name \"{getattr(arg,'name','<No Name>')}\" that does not exist in the namespace.\n"
                            "Did you make sure to include all PyBlocks and Queries in the func kwarg of Generator()?"
                        )
                    edges.append((arg.key, node_id))

        graph = DiGraph()
        for node_id, node in nodes.items():
            graph.add_node(node_id, data=node)
        graph.add_edges_from(edges)
        return graph

    def _sort_graph(self) -> List["ComputationalNode"]:
        graph = self._computational_graph()
        sorted_node_ids = topsort_with_dict(graph)
        sorted_nodes = [graph.nodes[key]["data"] for key in sorted_node_ids]
        return sorted_nodes

    def _sorted_loads(self) -> List[Load]:
        sorted_nodes = self._sort_graph()
        return [node for node in sorted_nodes if isinstance(node, Load)]

    def _get_dependency(self) -> Dependency:
        dep_list = [self.additional_dependencies] if self.additional_dependencies else []
        dep_list.extend([node._get_dependency() for node in self._sort_graph()])
        return reduce(lambda p, n: p.merge(n), dep_list, Dependency())

    def _get_gen_row(self) -> GeneratorEntity:
        # Assemble stringified dependency fields as we can't store sets in postgres easily
        deps = self._get_dependency()
        dep_kwargs = {}
        for x in deps.__fields__:
            dep_val = getattr(deps, x)
            dep_kwargs[x] = ",".join(dep_val) if dep_val else None
        return GeneratorEntity(
            id=self.uuid,
            name=self.name,
            description=self.description,
            tags=",".join(self.tags),
            query=self.extract.query if isinstance(self.extract, BaseQuery) else None,
            gen_json=self.json(),
            **dep_kwargs,
        )

    def run(
        self,
        main_engine: Engine,
        meta_engine: Engine,
        run_id: int = None,
        ordering: int = None,
        run_config=None,
    ):
        from dbgen.core.run import GeneratorRun

        return GeneratorRun(generator=self).execute(
            main_engine,
            meta_engine,
            run_id,
            run_config,
            ordering,
        )
