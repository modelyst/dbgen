from typing import Dict
from typing import Union as U

from hypothesis.strategies import SearchStrategy

from dbgen.core.schema import Attr, Obj, UserRel
from dbgen.core.schemaclass import Schema
from dbgen.utils.misc import Base
from .core.schema import AttrStrat, ObjStrat, UserRelStrat, SchemaStrat

STRATEGIES: Dict[str, U[SearchStrategy[SearchStrategy[Base]], SearchStrategy[Base]]] = {
    UserRel.canonical_name(): UserRelStrat(),
    Attr.canonical_name(): AttrStrat(),
    Obj.canonical_name(): ObjStrat(),
    Schema.canonical_name(): SchemaStrat(),
}


def get_strategy(
    dbgen_object,
) -> U[SearchStrategy[SearchStrategy[Base]], SearchStrategy[Base]]:
    """
    Retrieve the hypothesis strategy for a given object

    Args:
        dbgen_object (Base): DBgen object to extract the strategy for
    """
    canonical_name = dbgen_object.canonical_name()
    if canonical_name in STRATEGIES:
        return STRATEGIES[canonical_name]
    raise ValueError(f"Unknown canonical name: {canonical_name}")
