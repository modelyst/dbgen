from typing import List as L, Type
from hypothesis.strategies import SearchStrategy, one_of, recursive  # type: ignore
from dbgen.core.expr.pathattr import PathAttr
from dbgen.core.expr.expr import (
    Literal,
    ABS,
    SQRT,
    AND,
    OR,
    MAX,
    SUM,
    MIN,
    AVG,
    COUNT,
    BINARY,
    LEN,
    NOT,
    NULL,
    Unary,
    Binary,
    Nary,
    REGEXP,
    LIKE,
    MUL,
    DIV,
    PLUS,
    MINUS,
    POW,
    LEFT,
    RIGHT,
    JSON_EXTRACT,
    EQ,
    NE,
    LT,
    GT,
    LE,
    GE,
    CONCAT,
    COALESCE,
    Tup,
)


# jsonstrat = recursive(none() | booleans() | floats() | text(),
#                       lambda children: lists(children, 1) |
#                       dictionaries(text(), children, min_size=1))
def nullarystrat() -> SearchStrategy:
    return one_of(Literal.strat(), PathAttr.strat())


def unarystrat(x: SearchStrategy) -> SearchStrategy:
    uns = [
        ABS,
        SQRT,
        MAX,
        SUM,
        MIN,
        AVG,
        COUNT,
        BINARY,
        LEN,
        NOT,
        NULL,
    ]  # type: L[Type[Unary]]
    return one_of(*[u.strat(x) for u in uns])


def binarystrat(x: SearchStrategy) -> SearchStrategy:
    bins = [
        REGEXP,
        LIKE,
        MUL,
        DIV,
        PLUS,
        MINUS,
        POW,
        LEFT,
        RIGHT,
        JSON_EXTRACT,
        EQ,
        NE,
        LT,
        GT,
        LE,
        GE,
    ]  # type: L[Type[Binary]]
    return one_of(*[b.strat(x) for b in bins])


def narystrat(x: SearchStrategy) -> SearchStrategy:
    ns = [CONCAT, COALESCE, Tup, OR, AND]  # type: L[Type[Nary]]
    return one_of(*[n.strat(x) for n in ns])


def exprstrat() -> SearchStrategy:
    return recursive(
        nullarystrat(), lambda x: unarystrat(x) | binarystrat(x) | narystrat(x)
    )


if __name__ == "__main__":
    ns = nullarystrat().example()
    us = unarystrat(nullarystrat()).example()
    bs = binarystrat(nullarystrat()).example()
    Ns = narystrat(nullarystrat()).example()
    uus = unarystrat(unarystrat(nullarystrat())).example()
