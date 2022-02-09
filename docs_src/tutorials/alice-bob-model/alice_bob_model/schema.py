from typing import Optional
from uuid import UUID

from dbgen import Entity


class Person(Entity, table=True):
    __tablename__ = "person"
    __identifying__ = {"first_name", "last_name"}
    first_name: str
    last_name: str
    age: Optional[int]


class TemperatureMeasurement(Entity, table=True):
    __tablename__ = "temperature_measurement"
    __identifying__ = {"person_id", "ordering"}
    temperature_F: Optional[float]
    temperature_C: Optional[float]
    ordering: Optional[int]
    person_id: UUID = Person.foreign_key()
