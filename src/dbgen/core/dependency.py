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

from pprint import pformat
from typing import Set, Tuple

from pydantic import Field

from dbgen.core.base import Base

set_field = Field(default_factory=lambda: set())


class Dependency(Base):
    tables_needed: Set[str] = set_field
    columns_needed: Set[str] = set_field
    tables_yielded: Set[str] = set_field
    columns_yielded: Set[str] = set_field

    def all(self) -> Tuple[str, str, str, str]:
        a, b, c, d = tuple(
            map(
                lambda x: ",".join(sorted(x)),
                [
                    self.tables_needed,
                    self.columns_needed,
                    self.tables_yielded,
                    self.columns_yielded,
                ],
            )
        )
        return a, b, c, d

    def __str__(self) -> str:
        return pformat(self.__dict__)

    # Public Methods #

    def test(self, other: "Dependency") -> bool:
        """Test whether SELF depends on OTHER"""
        return not (
            self.tables_needed.isdisjoint(other.tables_yielded)
            and self.columns_needed.isdisjoint(other.columns_yielded)
        )

    def merge(self, other: "Dependency") -> "Dependency":
        """Combine two dependencies"""
        return Dependency(
            tables_needed=self.tables_needed.union(other.tables_needed),
            columns_needed=self.columns_needed.union(other.columns_needed),
            tables_yielded=self.tables_yielded.union(other.tables_yielded),
            columns_yielded=self.columns_yielded.union(other.columns_yielded),
        )
