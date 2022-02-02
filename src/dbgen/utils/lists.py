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

from collections.abc import Iterable
from functools import reduce
from itertools import repeat
from typing import Dict, Generator, Sequence, Set, Tuple, TypeVar, overload

##############################################################
T = TypeVar("T")
_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")
_T3 = TypeVar("_T3")
_T4 = TypeVar("_T4")
_T5 = TypeVar("_T5")


def is_broadcastable(*args) -> bool:
    """
    Enforce that all non-length-1 elements have the same length
    """
    length_set: Set[int] = reduce(lambda x, y: x.union([len(y)]), args, set())
    # Check lengths
    error_message = f"Found {len(length_set)-(1 in length_set)} different >1-length iterables, cannot broadcast: \nLengths: {length_set}"
    if not len(length_set) <= 2 or (len(length_set) > 1 and 1 not in length_set and 0 not in length_set):
        raise ValueError(error_message)
    return True


@overload
def broadcast(
    __vals1: Sequence[_T1],
    __vals2: Sequence[_T2],
) -> Generator[Tuple[_T1, _T2], None, None]:
    ...


@overload
def broadcast(
    __vals1: Sequence[_T1],
    __vals2: Sequence[_T2],
    __vals3: Sequence[_T3],
) -> Generator[Tuple[_T1, _T2, _T3], None, None]:
    ...


@overload
def broadcast(
    __vals1: Sequence[_T1],
    __vals2: Sequence[_T2],
    __vals3: Sequence[_T3],
    __vals4: Sequence[_T4],
) -> Generator[Tuple[_T1, _T2, _T3, _T4], None, None]:
    ...


@overload
def broadcast(
    __vals1: Sequence[_T1],
    __vals2: Sequence[_T2],
    __vals3: Sequence[_T3],
    __vals4: Sequence[_T4],
    __vals5: Sequence[_T5],
) -> Generator[Tuple[_T1, _T2, _T3, _T4, _T5], None, None]:
    ...


def broadcast(*args: Sequence[T]) -> Generator[Tuple[T, ...], None, None]:
    """Broadcast list of sequences together, requires all lists to be same length or length 1.

    Yields:
        Generator[Tuple[T, ...], None, None]: Generator that returns a tuple from each input Sequence
    """
    # Check that args are broadcastable
    is_broadcastable(*args)
    iterators = [iter(arg) for arg in args]
    num_active = len(iterators)
    empty_lists = [l for l in args if len(l) == 0]
    if empty_lists:
        # If there are empty lists return N empty lists
        yield from ([] for _ in range(num_active))  # type: ignore
        return
    fillvalue: Dict[int, T] = {}
    if not num_active:
        return
    while True:
        values = []
        for i, it in enumerate(iterators):
            try:
                value = next(it)
                fillvalue[i] = value
            except StopIteration:
                try:
                    num_active -= 1
                    if not num_active:
                        return
                    iterators[i] = repeat(fillvalue[i])
                    value = fillvalue[i]
                except KeyError:
                    raise ValueError(f"Error broadcasting lists: {args}")
            values.append(value)
        yield tuple(values)


def flatten(thing):
    for element in thing:
        if isinstance(element, Iterable) and not isinstance(element, str):
            yield from flatten(element)
        else:
            yield element
