from typing import Any
from json import dumps
from hashlib import sha256
Any
################################################################################
def hash_(x: 'Any') -> int:
    """
    NEW: Take a list of showable things and generate a unique hash value in
       longint range (-9223372036854775808 to +9223372036854775807)
    """
    return (int(sha256(dumps(x).encode('utf-8')).hexdigest(), 16) % 18446744073709551616) - 9223372036854775808


def abbreviate(x:'Any') -> 'Any':
    '''Truncate super long messages.'''
    if isinstance(x,str) and len(x) > 1000:
        return x[:1000]+'...\n\t'
    else:
        return x

def cap(x:str) -> str:
    '''Capitalize a string.'''
    return x[0].upper() + x[1:]

def levenshteinDistance(s1 : str, s2 : str) -> int:
    '''Distance between strings: # of characters difference'''
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
