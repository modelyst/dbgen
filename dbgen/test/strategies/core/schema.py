"""Store the hypothesis strategies for core dbgen objects"""
from typing import List as L
from typing import Tuple as T
from typing import Callable as C
from typing import Union as U

from hypothesis import infer, assume
from hypothesis.strategies import (
    SearchStrategy,
    builds,
    just,
    lists,
    composite,
    sampled_from,
)

from dbgen.core.schema import Attr, Obj, UserRel
from dbgen.core.schemaclass import Schema
from dbgen.test.strategies.utils import letters


def AttrStrat(name: U[str, SearchStrategy[str]] = None) -> SearchStrategy[Attr]:
    """Strategy for DBgen load object"""
    name = name or letters
    name_strat = just(name) if isinstance(name, str) else name
    return builds(Attr, name=name_strat, identifying=infer, desc=infer, dtype=infer,)


def UserRelStrat() -> SearchStrategy[UserRel]:
    """Strategy for the UserRel object"""
    return builds(UserRel)


@composite
def ObjStrat(
    draw: C, name: str = None, attrs: L[str] = None, fks: L[T[str, str]] = None,
) -> SearchStrategy[Obj]:
    """Strategy for DBgen Obj object"""
    MIN_ATTR, MAX_ATTR = [0, 2] if attrs is None else [len(attrs)] * 2
    MIN_FK, MAX_FK = [0, 2] if fks is None else [len(fks)] * 2
    size = 1 + MAX_ATTR + MAX_FK
    xx = draw(lists(letters, min_size=size, max_size=size, unique=True))
    if name is not None:
        assume(name not in xx)
    attrlist = draw(lists(AttrStrat(), min_size=MIN_ATTR, max_size=MAX_ATTR))
    for i, a in enumerate(attrlist):
        a.name = attrs[i] if attrs is not None else xx[1 + i]
    fklist = draw(lists(UserRelStrat(), min_size=MIN_FK, max_size=MAX_FK))
    for i, f, in enumerate(fklist):
        f.name, f.tar = (
            fks[i] if fks is not None else (xx[1 + MAX_ATTR + i], xx[1 + MAX_ATTR + i])
        )
    b = builds(
        Obj,
        name=just(name if name else xx[0]),
        desc=infer,
        attrs=just(attrs),
        fks=just(fklist),
    )
    return draw(b)


@composite
def SchemaStrat(
    draw: C, MAX_OBJ: int = None, MAX_FK: int = None
) -> SearchStrategy[Schema]:
    """Strategy for DBgen Obj object"""
    MAX_OBJ = MAX_OBJ or 2
    MAX_FK = MAX_FK or 2
    objnames = draw(lists(letters, min_size=1, max_size=MAX_OBJ, unique=True))
    objlist: L[Obj] = []
    for o in objnames:
        fktargets = draw(lists(sampled_from(objnames), min_size=1, max_size=MAX_FK))
        n = len(fktargets)
        fknames = draw(
            lists(letters, min_size=n, max_size=n, unique=True).filter(
                lambda fkn: o not in fkn
            )
        )
        objlist.append(draw(ObjStrat(name=o, fks=list(zip(fknames, fktargets)))))
    return draw(builds(Schema, objlist=just(objlist)))
