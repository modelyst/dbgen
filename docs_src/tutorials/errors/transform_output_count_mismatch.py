from typing import List

from dbgen import Entity, ETLStep, Extract, Model, transform


class Number(Entity, table=True):
    __identifying__ = {"integer"}
    integer: int
    bigger_integer: int
    smaller_integer: int


class IntExtract(Extract):
    n: int
    outputs: List[str] = ["i"]

    def extract(self):
        yield from range(self.n)


# The "outputs" list has only one output name, but the function returns
# two values. This is the problem!
# The correct line would be: @transform(outputs=["bigger_integer", "smaller_integer"])
@transform(outputs=['wrong'])
def alter_ints(integer: int):
    bigger_integer = integer + 1
    smaller_integer = integer - 1
    return bigger_integer, smaller_integer


def make_model() -> Model:
    model = Model(name="transform_output_count_mismatch")
    with model:
        with ETLStep(name="ints"):
            integer = IntExtract(n=10).results()
            bigger_integer, smaller_integer = alter_ints(integer).results()
            Number.load(
                insert=True, integer=integer, bigger_integer=bigger_integer, smaller_integer=smaller_integer
            )
    return model
