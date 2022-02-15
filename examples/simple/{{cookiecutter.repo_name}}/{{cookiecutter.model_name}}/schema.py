"""Entities within the Database"""
from dbgen import Entity


class MyTable(Entity, table=True):
    __identifying__ = {"label"}
    label: str
