from typing import Optional
from uuid import UUID

from dbgen import Entity


class Base(Entity):
    created: str
    created_by: str


class Sample(Base, table=True):
    label: str
    __identifying__ = {"label"}


class JVCurve(Base, table=True):
    full_path: str
    max_power_point: float
    short_circuit_current_density: float
    open_circuit_voltage: float
    fill_factor: float
    parent_id: Optional[UUID] = Sample.foreign_key()
    __identifying__ = {"full_path"}
