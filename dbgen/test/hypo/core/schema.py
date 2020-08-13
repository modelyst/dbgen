"""Store the hypothesis strategies for core dbgen objects"""
from typing import List as L
from typing import Tuple as T
from typing import Callable as C

from hypothesis import infer
from hypothesis.strategies import SearchStrategy, builds, just, lists, composite

from dbgen.core.schema import Attr, Obj, UserRel
from dbgen.test.hypo.utils import letters


def AttrStrat() -> SearchStrategy[Attr]:
    """Strategy for DBgen load object"""
    return builds(Attr, name=letters, identifying=infer, desc=infer, dtype=infer)


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

    xx = draw(
        lists(letters, min_size=size, max_size=size, unique=True).filter(
            lambda x: name not in x
        )
    )
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
