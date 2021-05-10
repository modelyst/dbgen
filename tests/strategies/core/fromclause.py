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

"""Store the hypothesis strategies for core dbgen objects"""

from hypothesis.strategies import SearchStrategy, builds, lists, sets

from dbgen.core.fromclause import From, Join, Path
from dbgen.core.schema import PathEQ
from dbgen.utils.misc import nonempty

# from .schema import UserRelStrat


def JoinStrat() -> SearchStrategy[Join]:
    """Strategy for the UserRel object"""
    return builds(
        Join,
        obj=nonempty,
        #  conds=dictionaries(JoinStrat(), UserRelStrat())
    )


def FromStrat() -> SearchStrategy[From]:
    return builds(
        From,
        basis=lists(nonempty, min_size=1, max_size=2),
        joins=sets(JoinStrat(), min_size=1, max_size=2),
    )


def PathStrat() -> SearchStrategy[Path]:
    return builds(Path, end=nonempty)


def PathEQStrat() -> SearchStrategy[PathEQ]:
    return builds(PathEQ, p1=PathStrat(), p2=PathStrat())
