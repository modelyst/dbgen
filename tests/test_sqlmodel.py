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

from datetime import datetime
from typing import Optional

from sqlmodel import Field, Session, SQLModel, create_engine, func, select


class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created: datetime
    deleted: bool = False
    category: str
    version: float = 1
    data: str


# Create and save records to show that the query itself is working.
item_1 = Item(created=datetime.now(), category="category_1", data="❤️ I love SQLModel.")
item_2 = Item(
    created=datetime.now(),
    category="category_1",
    data="❤️ I love FastAPI.",
    deleted=True,
)
item_3 = Item(
    created=datetime.now(),
    category="category_2",
    data="🥰 I appreciate your work on all of it!",
)

engine = create_engine("sqlite://")

SQLModel.metadata.create_all(engine)

with Session(engine) as session:
    session.add(item_1)
    session.add(item_2)
    session.add(item_3)
    session.commit()

    # This "statement" is where the issue presents itself in PyCharm
    statement = (
        select(
            Item.category,
            func.count(Item.id).label("my_count"),
            func.total(Item.deleted).label("delete_count"),
            func.min(Item.created).label("oldest_timestamp"),
            func.max(Item.created).label("newest_timestamp"),
            func.group_concat(Item.version).label("version_list"),
        )
        .distinct()
        .group_by(Item.category)
    )
    category_metadata = session.exec(statement)
    for result in category_metadata:
        print(dict(result))
