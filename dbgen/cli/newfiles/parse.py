"""parse_employees function"""
from typing import Tuple as T
from csv import reader


def parse_employees(pth: str) -> T[list, list, list, list, list]:
    """
    Takes in path to employees csv and returns a tuple of lists for each column

    Args:
        pth (str): path to employee csv

    Returns:
        T[list, list, list, list, list]: tuple of lists (employees, salary,
        managers, department, secretary)
    """
    with open(pth, "r") as f:
        r = reader(f)
        next(r)
        ename, sal, man, dname, sec = tuple(map(list, zip(*r)))  # type: ignore
    return ename, [float(s) for s in sal], man, dname, sec  # type: ignore
