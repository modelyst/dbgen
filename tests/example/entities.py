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

from typing import Optional
from uuid import UUID

from sqlalchemy.orm import registry
from sqlmodel import Field

from dbgen.core.entity import Entity

default_registry = registry()


class BaseTable(Entity, registry=default_registry):
    __identifying__ = {"type", "label"}
    type: str
    label: str


class GrandParent(BaseTable, table=True, registry=default_registry):
    pass


class Parent(BaseTable, table=True, registry=default_registry):
    grand_parent_id: Optional[UUID] = Field(None, foreign_key="public.grandparent.id")
    non_id: Optional[int] = None


class Child(BaseTable, table=True, registry=default_registry):
    parent_id: Optional[UUID] = Field(None, foreign_key="public.parent.id")
    uncle_id: Optional[UUID] = Field(None, foreign_key="public.parent.id")
