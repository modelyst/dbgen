# External Modules
from typing import List
from pprint import pformat
from networkx import DiGraph, NetworkXUnfeasible  # type: ignore
from networkx.algorithms import (  # type: ignore
    lexicographical_topological_sort,
    simple_cycles,
)

# Graph
# --------
def topsort_with_dict(G: DiGraph, d: dict) -> List:
    """
    Assuming a graph with object names and dict mapping names to objects,
    perform a topsort and return the list of objects.
    """
    try:
        sortd = list(lexicographical_topological_sort(G))
        return [d[x] for x in sortd]
    except NetworkXUnfeasible:
        cycles = pformat(list(simple_cycles(G)))
        raise ValueError("Cycles found: %s" % cycles)


def make_acyclic(G: DiGraph) -> DiGraph:
    stack = list(simple_cycles(G))
    while stack:
        cyc = stack.pop()
        G.remove_edge(cyc[0], cyc[1])  # this is bad, remove random edge
        stack = list(simple_cycles(G))
    return G
