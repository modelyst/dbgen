from dbgen import Entity


class Sample(Entity, table=True):
    label: str
    created: str
    created_by: str
    __identifying__ = {"label"}
