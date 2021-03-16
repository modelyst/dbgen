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

from typing import List as L
from typing import Type

from hypothesis.strategies import SearchStrategy, one_of, recursive

from dbgen.core.expr.expr import (
    ABS,
    AND,
    AVG,
    BINARY,
    COALESCE,
    CONCAT,
    COUNT,
    DIV,
    EQ,
    GE,
    GT,
    JSON_EXTRACT,
    LE,
    LEFT,
    LEN,
    LIKE,
    LT,
    MAX,
    MIN,
    MINUS,
    MUL,
    NE,
    NOT,
    NULL,
    OR,
    PLUS,
    POW,
    REGEXP,
    RIGHT,
    SQRT,
    SUM,
    Binary,
    Literal,
    Nary,
    Tup,
    Unary,
)
from dbgen.core.expr.pathattr import PathAttr


# jsonstrat = recursive(none() | booleans() | floats() | text(),
#                       lambda children: lists(children, 1) |
#                       dictionaries(text(), children, min_size=1))
def nullarystrat() -> SearchStrategy:
    return one_of(Literal._strat(), PathAttr._strat())


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
    return one_of(*[u._strat(x) for u in uns])


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
    return one_of(*[b._strat(x) for b in bins])


def narystrat(x: SearchStrategy) -> SearchStrategy:
    ns = [CONCAT, COALESCE, Tup, OR, AND]  # type: L[Type[Nary]]
    return one_of(*[n._strat(x) for n in ns])


def exprstrat() -> SearchStrategy:
    return recursive(nullarystrat(), lambda x: unarystrat(x) | binarystrat(x) | narystrat(x))


if __name__ == "__main__":
    ns = nullarystrat().example()
    us = unarystrat(nullarystrat()).example()
    bs = binarystrat(nullarystrat()).example()
    Ns = narystrat(nullarystrat()).example()
    uus = unarystrat(unarystrat(nullarystrat())).example()
