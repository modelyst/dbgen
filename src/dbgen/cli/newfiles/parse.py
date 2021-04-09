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

"""parse_employees function"""
from csv import reader
from typing import Tuple as T


def parse_employees(pth: str) -> T[list, list, list, list, list]:
    """
    Takes in path to employees csv and returns a tuple of lists for each column

    Args:
        pth (str): path to employee csv

    Returns:
        T[list, list, list, list, list]: tuple of lists (employees, salary,
        managers, department, secretary)
    """
    with open(pth) as f:
        r = reader(f)
        next(r)
        ename, sal, man, dname, sec = tuple(map(list, zip(*r)))  # type: ignore
    return ename, [float(s) for s in sal], man, dname, sec  # type: ignore
