from dbgen import Entity
from typing import Optional


class Person(Entity, table=True):
    __tablename__ = "person"
    __identifying__ = {"first_name", "last_name"}
    first_name: str
    last_name: str
    age: Optional[int]