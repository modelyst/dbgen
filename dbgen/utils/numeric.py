from typing import Any, List

from fractions import Fraction, gcd
from functools import reduce

# Numeric functions
# ------------------
def lcm(a: int, b: int)->int:
    return a * b // gcd(a, b)


def common_integer(numbers: List[float])->List[int]:
    fractions = [Fraction(n).limit_denominator() for n in numbers]
    multiple = reduce(lcm, [f.denominator for f in fractions])
    ints = [f * multiple for f in fractions]
    divisor = reduce(gcd, ints)
    return [int(n / divisor) for n in ints]


def roundfloat(x: Any) -> float:
    output = round(float(x), 3)
    return abs(output) if output == 0 else output


def safe_div(x: float, y: float)->float:
    try:
        return x / y
    except ZeroDivisionError:
        return 0.0
