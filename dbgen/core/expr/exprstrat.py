from hypothesis.strategies import SearchStrategy, one_of, recursive # type: ignore
from dbgen.core.expr.pathattr import PathAttr
from dbgen.core.expr.expr import Literal, ABS, SQRT


# jsonstrat = recursive(none() | booleans() | floats() | text(),
#                       lambda children: lists(children, 1) |
#                       dictionaries(text(), children, min_size=1))
def nullarystrat() -> SearchStrategy:
    return one_of(Literal.strat(), PathAttr.strat())

def unarystrat(x:SearchStrategy) -> SearchStrategy:
    return one_of(ABS.strat(x), SQRT.strat(x))
# def binarystrat(x:SearchStrategy[Expr]) -> SearchStrategy[Expr]:
#     raise NotImplementedError
# def narystrat(x:SearchStrategy[Expr]) -> SearchStrategy[Expr]:
#     l = lists(x,min_size=1,max_size=2)
#     raise NotImplementedError

def exprstrat() -> SearchStrategy:
    return recursive(nullarystrat(),
                      lambda x: unarystrat(x))# | binarystrat(x) | narystrat(x))
