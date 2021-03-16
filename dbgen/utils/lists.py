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

"""Common list utilities for flattening, concating, and broadcasting."""
from datetime import datetime
from decimal import Decimal
from typing import Any
from typing import Callable as C
from typing import Dict as D
from typing import Iterable
from typing import List as L
from typing import TypeVar
from typing import Union as U

##############################################################################
A = TypeVar("A")
B = TypeVar("B")
##############################################################
def flatten(lol: L[L[A]]) -> L[A]:
    """Convert list of lists to a single list via concatenation"""
    return [item for sublist in lol for item in sublist]


##############################################################
def nub(seq: L[A], idfun: C = None) -> L[A]:
    """
    Remove duplicates but preserve order.
    """
    if idfun is None:

        def f(x: A) -> A:
            return x

    else:
        f = idfun
    seen = {}  # type: D[A,int]
    result = []  # type: L[A]
    for item in seq:
        marker = f(item)
        if marker in seen:
            continue
        seen[marker] = 1
        result.append(item)
    return result


##############################################################
def merge_dicts(dicts: L[D[A, B]]) -> D[A, B]:
    return {k: v for d in dicts for k, v in d.items()}


##############################################################
def concat_map(f: C[[A], L[B]], args: L[A]) -> L[B]:
    """
    Maps a function over an input.
    We apply the function to every element in the list and concatenate result.
    """
    return flatten([f(arg) for arg in args])


##############################################################
valid_types = (
    int,
    str,
    tuple,
    float,
    list,
    bytes,
    datetime,
    Decimal,
    type(None),
)


def broadcast(args: L[U[L[B], A]]):
    """
    Enforce that all non-length-1 elements have the same length and replicate
    length-1 elements to the largest length list, then zip all the lists
    """

    type_err = "Arg (%s) BAD DATATYPE %s IN NAMESPACE "
    broad_err = "Can't broadcast: maxlen = %d, len a = %d (%s)"
    maxlen = 1  # initialize variable

    for a in args:
        assert isinstance(a, valid_types), type_err % (a, a.__class__)

        if isinstance(a, (list, tuple)):
            if maxlen != 1:  # variable has been set
                # preconditions for broadcasting
                assert len(a) in [1, maxlen], broad_err % (
                    maxlen,
                    len(a),
                    str(a),
                )
            else:
                maxlen = len(a)  # set variable for first (and last) time

    def process_arg(x: U[L[B], A]) -> U[L[B], L[A]]:
        if isinstance(x, (list, tuple)) and len(x) != maxlen:
            return maxlen * list(x)  # broadcast
        elif not isinstance(x, list):
            return maxlen * [x]
        else:
            return x

    # now all args should be lists of the same length
    broadcasted = [process_arg(x) for x in args]
    return list(zip(*broadcasted))


##############################################################
def batch(iterable: list, n: int = 1) -> Iterable:
    """
    returns an iterable that where every iteration
    returns n of items from the original list
    """
    iter_length = len(iterable)
    for ndx in range(0, iter_length, n):
        yield iterable[ndx : min(ndx + n, iter_length)]


##############################################################
def is_iterable(potentially_iterable: Any) -> bool:
    """
    Tests iterability for better error messaging.

    Args:
        potentially_iterable (Any): variable to test for iterability

    Returns:
        bool: potentially_iterable is iterable
    """
    try:
        iter(potentially_iterable)
    except TypeError:
        return False
    return True
