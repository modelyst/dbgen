import re
from typing import List, Tuple

from dbgen import Entity, Environment, ETLStep, Extract, Import, Model, transform


class StringAndInt(Entity, table=True):
    __tablename__ = "number"
    __identifying__ = {"i", "s"}
    i: int
    s: str


class StringExtract(Extract):
    n: int
    outputs: List[str] = ["a_string"]

    def extract(self):
        for i in range(self.n):
            a_string = f"string:{i}"
            yield {"a_string": a_string}


### Env does not include the "re" package! This is the source of the error!
env = Environment([Import("typing", "Tuple")])

### The correct Env line is shown below
# env = Env([Import("typing", "Tuple"), Import("re")])


@transform(outputs=["s", "i"], env=env)
def parse_string(inp: str) -> Tuple[str, int]:
    regex = r"([a-z]+):(\d+)"
    match = re.match(regex, inp)
    s, i_str = match.groups()
    return s, int(i_str)


def make_model() -> Model:
    model = Model(name="only_one_row")
    with model:
        with ETLStep(name="ints"):
            a_string = StringExtract(n=10).results()
            s, i = parse_string(a_string).results()
            StringAndInt.load(insert=True, i=i, s=s)
    return model
