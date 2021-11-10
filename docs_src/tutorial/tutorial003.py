from typing import Optional
from uuid import UUID

from dbgen import Entity


class Sample(Entity, table=True):
    label: str
    created: str
    created_by: str
    __identifying__ = {"label"}


class JVCurve(Entity, table=True):
    full_path: str
    created: str
    created_by: str
    max_power_point: float
    short_circuit_current_density: float
    open_circuit_voltage: float
    fill_factor: float
    parent_id: Optional[UUID] = Sample.foreign_key()
    __identifying__ = {"full_path"}
