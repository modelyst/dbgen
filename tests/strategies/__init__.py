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

from typing import Dict
from typing import Union as U

from hypothesis.strategies import SearchStrategy

from dbgen.core.schema import Attr, Entity, UserRel
from dbgen.core.schemaclass import Schema
from dbgen.utils.misc import Base

from .core.schema import AttrStrat, ObjStrat, SchemaStrat, UserRelStrat

STRATEGIES: Dict[str, U[SearchStrategy[SearchStrategy[Base]], SearchStrategy[Base]]] = {
    UserRel.canonical_name(): UserRelStrat(),
    Attr.canonical_name(): AttrStrat(),
    Entity.canonical_name(): ObjStrat(),
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
