from typing import List

from dbgen import Entity, ETLStep, Extract, Model


class Number(Entity, table=True):
    __identifying__ = {"integer"}
    integer: int
    bigger_integer: int


class IntExtract(Extract):
    n: int
    outputs: List[str] = ["integer", "bigger_integer"]

    def extract(self):
        for i in range(self.n):
            yield i, i + 1


def make_model() -> Model:
    model = Model(name="missing_id_info")
    with model:
        with ETLStep(name="ints"):
            integer, bigger_integer = IntExtract(n=10).results()
            # The load statement does not supply the identifying information,
            # which is set in "__identifying__={...}"
            # A valid load statement would be:
            # Number.load(insert=True, integer=integer, bigger_integer=bigger_integer)
            Number.load(insert=True, bigger_integer=bigger_integer)
    return model
