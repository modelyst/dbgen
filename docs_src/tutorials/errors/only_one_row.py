from typing import List

from dbgen import Entity, ETLStep, Extract, Model


class Number(Entity, table=True):
    # __identifying__ is not set! This is the problem.
    i: int


class IntExtract(Extract):
    n: int
    outputs: List[str] = ["i"]

    def extract(self):
        yield from range(self.n)


def make_model() -> Model:
    model = Model(name="only_one_row")
    with model:
        with ETLStep(name="ints"):
            i = IntExtract(n=10).results()
            Number.load(insert=True, i=i)
    return model
