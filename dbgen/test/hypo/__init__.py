from typing import Dict, Union as U
from hypothesis.strategies import SearchStrategy
from dbgen.utils.misc import Base
from .core.schema import UserRelStrat, AttrStrat, ObjStrat

STRATEGIES: Dict[str, U[SearchStrategy[SearchStrategy[Base]], SearchStrategy[Base]]] = {
    "UserRel": UserRelStrat(),
    "Attr": AttrStrat(),
    "Obj": ObjStrat(),
}
