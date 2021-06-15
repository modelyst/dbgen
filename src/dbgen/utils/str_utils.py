#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from hashlib import sha256
from json import dumps
from typing import Any

Any
################################################################################
# Number of hash values
HASH_COUNT = 18446744073709551616


def hash_(x: "Any") -> str:
    """
    NEW: Take a list of showable things and generate a unique hash value in
       longint range (-9223372036854775808 to +9223372036854775807)
    """
    json_string = dumps(x, sort_keys=True, indent=4, separators=(",", ": "))
    return hashdata_(json_string)


def hashdata_(input_data: "Any") -> str:
    """
    OLD VERSION: Creates a 128 Byte hash value #return sha512(str(x).encode()).hexdigest()
    NEW: Take a list of showable things and generate a unique hash value in
       longint range (-9223372036854775808 to +9223372036854775807)
    """
    encoded_string = str(input_data).encode("utf-8")
    hex_hash = sha256(encoded_string).hexdigest()
    int_hash = int(hex_hash, 16)
    converted_val = str((int_hash % HASH_COUNT) - int(HASH_COUNT / 2))
    return converted_val


def abbreviate(x: "Any") -> "Any":
    """Truncate super long messages."""
    if isinstance(x, str) and len(x) > 1000:
        return x[:1000] + "...\n\t"
    else:
        return x


def cap(x: str) -> str:
    """Capitalize a string."""
    return x[0].upper() + x[1:]


def levenshteinDistance(s1: str, s2: str) -> int:
    """Distance between strings: # of characters difference"""
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    distances = list(range(len(s1) + 1))
    for i2, c2 in enumerate(s2):
        distances_ = [i2 + 1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        distances = distances_
    return distances[-1]
