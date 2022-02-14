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
import traceback
from bdb import BdbQuit
from functools import reduce
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union
from uuid import UUID

from pydantic import Field, PrivateAttr
from pydantic.class_validators import validator
from sqlalchemy.future import Engine

from dbgen.core.args import Arg
from dbgen.core.base import Base
from dbgen.core.context import ETLStepContext, ModelContext
from dbgen.core.decorators import FunctionNode
from dbgen.core.dependency import Dependency
from dbgen.core.metadata import ETLStepEntity
from dbgen.core.node.extract import Extract
from dbgen.core.node.load import Load
from dbgen.core.node.query import BaseQuery
from dbgen.core.node.transforms import Transform
from dbgen.exceptions import DBgenMissingInfo, DBgenSkipException
from dbgen.utils.graphs import topsort_with_dict

if TYPE_CHECKING:
    from networkx import DiGraph  # pragma: no cover

    from dbgen.core.node.computational_node import ComputationalNode  # pragma: no cover

list_field = Field(default_factory=lambda: [])

NAME_REGEX = re.compile(r'^[\w.-]+$')
DEFAULT_EXTRACT: Extract[dict] = Extract()


class ETLStep(Base):
    name: str
    description: str = "<no description>"
    extract: Union[BaseQuery, Extract] = Field(default_factory=Extract)
    transforms: List[Transform] = list_field
    loads: List[Load] = list_field
    tags: List[str] = list_field
    batch_size: Optional[int] = None
    additional_dependencies: Optional[Dependency] = None
    dependency: Optional[Dependency] = None
    _graph: Optional["DiGraph"] = PrivateAttr(None)
    _context: ETLStepContext = PrivateAttr(None)
    _hashexclude_ = {
        'dependency',
    }

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, **kwargs)
        etl_step_context = ETLStepContext.get()
        if not etl_step_context:
            self.validate_nodes()
            self._sort_graph()

        model_context = ModelContext.get()
        if model_context:
            model = model_context['model']
            model.add_etl_step(self)

    @validator('name')
    def validate_etl_step_name(cls, name):
        if not NAME_REGEX.match(name):
            raise ValueError(
                f"ETLStep names have to be alphanumeric characters, dashes, dots, and underscores. No spaces allowed!\n  Offending Name: {name}"
            )
        return name

    @validator('transforms', pre=True)
    def convert_functional_node(cls, transforms):
        return [val.pyblock if isinstance(val, FunctionNode) else val for val in transforms]

    def validate_nodes(self):
        nodes = self.loads + self.transforms + [self.extract]
        hashes = {node.hash for node in nodes}

        for node in nodes:
            for arg in node.inputs.values():
                if isinstance(arg, Arg) and arg.key not in hashes:
                    if arg.name.endswith('_id'):
                        hint = f"Arg name being asked for is '{arg.name}' which matches the pattern of a Load for Entity with name {arg.name[:-3]!r}. Are you missing a load?"
                    elif self.extract.hash == Extract().hash:
                        hint = "ETLStep is using the default extract, did you remember your query or extractor to the extract field?"
                    else:
                        hint = "The arg details seem to match a transform, did you add all pyblocks?"
                    raise ValueError(
                        f"ETLStep(name={self.name!r}) encountered a validation error as a node is missing in the computational graph.\n  "
                        f"Node {node} is looking for an output named {arg.name!r} on another node with a hash {arg.key!r}\n  "
                        + hint
                    )

    def __str__(self) -> str:
        return f"ETLStep<{self.name}>"

    def __enter__(self) -> "ETLStep":
        self._context = ETLStepContext(context_dict={'etl_step': self})
        return self._context.__enter__()['etl_step']

    def __exit__(self, *args):
        self._context.__exit__(*args)
        self.validate_nodes()
        self._sort_graph()
        del self._context

    def transform_batch(self, batch: List[Tuple[UUID, Dict[str, Dict[str, Any]]]]):
        """Transform a batch of extracted namespaces."""
        # initialize the master dict for the rows that will need to be loaded after
        rows_to_load: Dict[str, Dict[UUID, dict]] = {node.hash: {} for node in self.loads}
        processed_hashes = []
        inputs_skipped = 0
        for input_hash, row in batch:
            try:
                _, skipped = self._transform(row, rows_to_load)
                if not skipped:
                    processed_hashes.append(input_hash)
                else:
                    inputs_skipped += 1
            except (KeyboardInterrupt, SystemExit, BdbQuit):
                raise
            except BaseException:
                return None, None, None, inputs_skipped, traceback.format_exc()
        return processed_hashes, rows_to_load, len(batch), inputs_skipped, None

    def _transform(self, namespace: dict, rows_to_load: Dict[str, Dict[UUID, dict]]):
        skipped = False
        try:
            output = namespace.copy()
            # Run the Transforms
            for node in self.transforms:
                output[node.hash] = node.run(output)
            # Run the loads
            for load in self.loads:
                output[load.hash] = load.new_run(output, rows_to_load)
        except DBgenSkipException as exc:
            self._logger.debug(f'Skipped row: {exc.msg}')
            skipped = True
        return output, skipped

    def _sort_graph(self):
        """Sorts the self.transforms and self.loads to be in correct DAG order."""
        nodes = self._sort_nodes(force_rebuild=True)
        transforms, loads = [], []
        for node in nodes:
            if isinstance(node, Transform):
                transforms.append(node)
            elif isinstance(node, Load):
                loads.append(node)
            elif not isinstance(node, Extract):
                raise TypeError(f"Unknown node type found during sorting! {type(node)}")
        self.transforms = transforms
        self.loads = loads

    def _computational_graph(self, force_rebuild: bool = True) -> "DiGraph":
        if self._graph is None or force_rebuild:
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
            self._graph = graph
        return self._graph

    def _sort_nodes(self, force_rebuild: bool = False) -> List["ComputationalNode"]:
        graph = self._computational_graph(force_rebuild=force_rebuild)
        sorted_node_ids = topsort_with_dict(graph)
        sorted_nodes = [
            graph.nodes[key]["data"]
            for key in sorted_node_ids
            if not isinstance(graph.nodes[key]["data"], Extract)
        ]
        return [self.extract, *sorted_nodes]

    def _sorted_loads(self) -> List[Load]:
        sorted_nodes = self._sort_nodes()
        return [node for node in sorted_nodes if isinstance(node, Load)]

    def _get_dependency(self) -> Dependency:
        dep_list = [self.additional_dependencies] if self.additional_dependencies else []
        dep_list.extend([node._get_dependency() for node in self._sort_nodes()])
        self.dependency = reduce(lambda p, n: p.merge(n), dep_list, Dependency())
        return self.dependency

    def _get_etl_step_row(self) -> ETLStepEntity:
        # Assemble stringified dependency fields as we can't store sets in postgres easily
        deps = self._get_dependency()
        dep_kwargs = {}
        for x in deps.__fields__:
            dep_val = getattr(deps, x)
            dep_kwargs[x] = ",".join(dep_val) if dep_val else None
        return ETLStepEntity(
            id=self.uuid,
            name=self.name,
            description=self.description,
            tags=",".join(self.tags),
            query=self.extract.query if isinstance(self.extract, BaseQuery) else None,
            etl_step_json=self.serialize(),
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
        from dbgen.core.run import ETLStepRun

        return ETLStepRun(etl_step=self).execute(main_engine, meta_engine, run_id, run_config, ordering)

    def add_node(self, node: 'ComputationalNode') -> None:
        if isinstance(node, Extract):
            if self.extract.__class__ == DEFAULT_EXTRACT.__class__:
                self.extract = node
            else:
                raise ValueError(
                    f"Can only define 1 extractor per etl_step\n"
                    f"{self.extract} already defined defined on {self}\n"
                    f"Cannot add extract {node}"
                )
        elif isinstance(node, Transform):
            self.transforms.append(node)
        elif isinstance(node, FunctionNode):
            self.transforms.append(node.pyblock)
        elif isinstance(node, Load):
            self.loads.append(node)
        else:
            raise ValueError(f"Unknown Node Type {node} {type(node)}")
