from typing import Any
from json import dumps
from hashlib import sha256
Any
################################################################################
# Number of hash values
HASH_COUNT = 18446744073709551616
def hash_(x: 'Any') -> str:
    """
    NEW: Take a list of showable things and generate a unique hash value in
       longint range (-9223372036854775808 to +9223372036854775807)
    """

    json_string = dumps(x, sort_keys=True, indent=4, separators=(',', ': '))
    # print(x)
    # print("JSON STRING FOR HASH:\n",json_string)
    encoded_json_string = json_string.encode('utf-8')
    hex_hash = sha256(encoded_json_string).hexdigest()
    int_hash = int(hex_hash, 16)
    converted_val = str(( int_hash % HASH_COUNT) - int(HASH_COUNT/2))
    return converted_val


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

if __name__ == '__main__':
    from dbgen.core.expr.sqltypes import Decimal
    test = Decimal().hash
    print(test)
