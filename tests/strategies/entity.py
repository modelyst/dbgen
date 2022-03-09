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

from string import ascii_lowercase
from typing import Any, Dict, Tuple, Type, TypeVar
from uuid import UUID

from hypothesis import strategies as st
from hypothesis.strategies._internal.strategies import SearchStrategy
from sqlalchemy.orm import registry
from sqlmodel import Field

from dbgen.core.entity import BaseEntity, EntityMetaclass

protected_words = {"mro"}
uni_text = lambda x: st.text(ascii_lowercase, min_size=x)
non_private_attr = uni_text(1).filter(lambda x: x[0] != "_").filter(lambda x: x not in protected_words)
pydantic_type_strat = st.sampled_from((str, float, bool, int))

id_field = Field(
    default=None,
    primary_key=True,
    sa_column_kwargs={"autoincrement": False, "unique": True},
)
fk_field = lambda x: Field(default=None, foreign_key=x)
ID_TYPE = UUID


reserved_words = {'hex', 'uuid', 'hash', 'id'}


@st.composite
def example_entity(
    draw,
    class_name: str = None,
    fks: Dict[str, str] = None,
    attrs: Dict[str, Tuple[type, Any]] = None,
    draw_attrs: bool = True,
    registry_: registry = None,
) -> SearchStrategy[Type[BaseEntity]]:
    class_name = class_name or draw(uni_text(1))
    if fks is None:
        fks = draw(
            st.dictionaries(
                non_private_attr.filter(
                    lambda x: attrs and x not in attrs and x != 'id' and x not in reserved_words
                ),
                non_private_attr,
            )
        )

    annotations: Dict[str, type] = {"id": UUID}
    if draw_attrs:
        annotations.update(
            draw(
                st.dictionaries(
                    non_private_attr.filter(lambda x: x not in fks and x not in reserved_words),
                    pydantic_type_strat,
                    min_size=1,
                )
            )
        )

    added_attrs = {"id": id_field}
    for fk_name, fk_col_reference in fks.items():
        annotations[fk_name] = UUID
        added_attrs[fk_name] = fk_field(fk_col_reference)

    attrs = attrs or {}
    for attr_name, attr_dets in attrs.items():
        if len(attr_dets) == 1:
            type_ = attr_dets[0]
        else:
            type_, default = attr_dets
            added_attrs[attr_name] = default

        annotations[attr_name] = type_
    identifying = draw(st.sets(st.sampled_from(list(annotations.keys()))))

    data = {
        "__annotations__": annotations,
        "__identifying__": identifying,
        "__module__": "tests.strategies.entity",
        "__qualname__": class_name,
        "__tablename__": f"table_{class_name}",
        **added_attrs,
    }
    new_class = EntityMetaclass(
        class_name,
        (BaseEntity,),
        data,
        table=True,
        registry=registry_ or registry(),
        force_validation=True,
    )
    return new_class


T = TypeVar("T")


def fill_required_fields(
    entity_class: Type[BaseEntity],
    default_values={},
):
    required_fields = [
        (name, default_values.get(val.type_, val.type_))
        for name, val in entity_class.__fields__.items()
        if val.required
    ]
    return entity_class.validate({x: y() for x, y in required_fields})
