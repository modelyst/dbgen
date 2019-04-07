from typing import Any
from hashlib import sha512
Any
################################################################################
def hash_(x:'Any')->str:
    return sha512(str(x).encode()).hexdigest()


def abbreviate(x:'Any')->'Any':
    if isinstance(x,str) and len(x) > 1000:
        return x[:1000]+'...\n\t'
    else:
        return x

def cap(x:str)->str:
    return x[0].upper() + x[1:]

def levenshteinDistance(s1 : str, s2 : str) -> int:
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = list(range(len(s1) + 1))
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        distances = distances_
    return distances[-1]
