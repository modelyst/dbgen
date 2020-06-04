from typing import List as L, Tuple as T
from csv import reader

def parse_employees(pth : str) -> T[L[str],L[float],L[str],L[str],L[str]]:
    with open(pth,'r') as f:
        r = reader(f); next(r)
        ename,sal,man,dname,sec = tuple(map(list,zip(*r))) # type: T[L[str],L[str],L[str],L[str],L[str]]
    return ename,[float(s) for s in sal],man, dname, sec
