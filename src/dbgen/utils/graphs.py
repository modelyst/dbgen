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

from pprint import pformat
from typing import Any, Callable, Dict, List, Set, Tuple, Union
from uuid import UUID

from networkx import DiGraph, NetworkXUnfeasible
from networkx.algorithms import lexicographical_topological_sort, simple_cycles
from pydantic import BaseModel


# Graph
# --------
def topsort_with_dict(G: DiGraph) -> List:
    """
    Assuming a graph with object names and dict mapping names to objects,
    perform a topsort and return the list of objects.
    """
    try:
        sortd = list(lexicographical_topological_sort(G))
        return sortd
    except NetworkXUnfeasible:
        cycles = pformat(list(simple_cycles(G)))
        raise ValueError(f"Cycles found: {cycles}")


class SerializedNode(BaseModel):
    id: UUID
    name: str
    dependency: Dict[str, Set[str]]


class SerializedGraph(BaseModel):
    metadata: Dict[str, Union[str, float, int, None]]
    nodes: Dict[str, SerializedNode]
    edges: List[Tuple[str, str]]


def serialize_graph(
    graph,
    node_serializer: Callable[[Any], SerializedNode],
    metadata: Dict[str, Union[str, float, int, None]] = None,
) -> Dict[str, Any]:
    metadata = metadata or {}
    output: SerializedGraph = SerializedGraph(nodes={}, edges=[], metadata=metadata)
    for key, data in graph.nodes.items():
        node = data
        output.nodes[key] = node_serializer(node)
    for source, target in graph.edges:
        assert isinstance(source, str) and isinstance(
            target, str
        ), f"Edge keys not strings! {type(source)} {type(target)}"
        output.edges.append((source, target))
    return output.dict()


# def topological_sort(G, key=None):
#     """Returns a etl_step of nodes in lexicographically topologically sorted
#     order.

#     A topological sort is a nonunique permutation of the nodes such that an
#     edge from u to v implies that u appears before v in the topological sort
#     order.

#     Parameters
#     ----------
#     G : NetworkX digraph
#         A directed acyclic graph (DAG)

#     key : function, optional
#         This function maps nodes to keys with which to resolve ambiguities in
#         the sort order.  Defaults to the identity function.

#     Returns
#     -------
#     iterable
#         An iterable of node names in lexicographical topological sort order.

#     Raises
#     ------
#     NetworkXError
#         Topological sort is defined for directed graphs only. If the graph `G`
#         is undirected, a :exc:`NetworkXError` is raised.

#     NetworkXUnfeasible
#         If `G` is not a directed acyclic graph (DAG) no topological sort exists
#         and a :exc:`NetworkXUnfeasible` exception is raised.  This can also be
#         raised if `G` is changed while the returned iterator is being processed

#     RuntimeError
#         If `G` is changed while the returned iterator is being processed.

#     Notes
#     -----
#     This algorithm is based on a description and proof in
#     "Introduction to Algorithms: A Creative Approach" [1]_ .

#     See also
#     --------
#     topological_sort

#     References
#     ----------
#     .. [1] Manber, U. (1989).
#        *Introduction to Algorithms - A Creative Approach.* Addison-Wesley.
#     """
#     if not G.is_directed():
#         msg = "Topological sort not defined on undirected graphs."
#         raise nx.NetworkXError(msg)

#     if key is None:

#         def key(node):
#             return node

#     nodeid_map = {n: i for i, n in enumerate(G)}

#     def create_tuple(node):
#         return key(node), nodeid_map[node], node

#     indegree_map = {v: d for v, d in G.in_degree() if d > 0}
#     # These nodes have zero indegree and ready to be returned.
#     zero_indegree = [create_tuple(v) for v, d in G.in_degree() if d == 0]
#     heapq.heapify(zero_indegree)

#     while zero_indegree:
#         _, _, node = heapq.heappop(zero_indegree)

#         if node not in G:
#             raise RuntimeError("Graph changed during iteration")
#         for _, child in G.edges(node):
#             try:
#                 indegree_map[child] -= 1
#             except KeyError as e:
#                 raise RuntimeError("Graph changed during iteration") from e
#             if indegree_map[child] == 0:
#                 heapq.heappush(zero_indegree, create_tuple(child))
#                 del indegree_map[child]

#         yield node

#     if indegree_map:
#         msg = "Graph contains a cycle or graph changed during iteration"
#         raise nx.NetworkXUnfeasible(msg)
