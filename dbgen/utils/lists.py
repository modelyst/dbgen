from typing  import (Any, TypeVar,
                     List     as L,
                     Dict     as D,
                     Callable as C)
from decimal import Decimal
##############################################################################
A = TypeVar('A')
B = TypeVar('B')
##############################################################
def flatten(lol: L[L[A]])->L[A]:
    return [item for sublist in lol for item in sublist]
##############################################################
def nub(seq: L[A], idfun : C = None)->L[A]:
    if idfun is None:
        def f(x:A)->A: return x
    else:
        f = idfun
    seen = {} # type: D[A,int]
    result = [] # type: L[A]
    for item in seq:
        marker = f(item)
        if marker in seen: continue
        seen[marker] = 1
        result.append(item)
    return result
##############################################################
def merge_dicts(dicts: L[D[A, B]])->D[A, B]:
    return {k: v for d in dicts for k, v in d.items()}

##############################################################
def concat_map(f: C[[A], L[B]], args: L[A]) -> L[B]:
    return flatten([f(arg) for arg in args])

##############################################################
def broadcast(dic:dict,xs:L[str])->list:
    valid_types = (int,str,tuple,float,list,bytes,Decimal,type(None))
    type_err    = "Arg (%s) BAD DATATYPE %s IN NAMESPACE "
    broad_err   = "Can't broadcast: maxlen = %d, len a = %d (%s)"
    maxlen = 1 # initialize variable

    missing_keys = set(xs) - set(dic.keys())

    assert not missing_keys, 'missing keys %s'%missing_keys

    args = [dic[x] for x in xs]

    for a in args:
        assert isinstance(a,valid_types), type_err%(a,a.__class__)

        if isinstance(a,(list,tuple)):
            if maxlen != 1: # variable has been set
                # preconditions for broadcasting
                assert(len(a) in [1,maxlen]), broad_err%(maxlen,len(a),str(a))
            else:
                maxlen = len(a) # set variable for first (and last) time

    def process_arg(x:Any)->list:
        if isinstance(x,(list,tuple)) and len(x)!=maxlen:
            return maxlen*list(x) # broadcast
        elif not isinstance(x,list):
            return  maxlen * [x]
        else:
            return x

    # now all args should be lists of the same length
    broadcasted = [process_arg(x) for x in args]

    binds = list(zip(*broadcasted))

    return binds
##############################################################

def gcd(args: L[Any])->Any:
    """
    Greatest common denominator of a list
    """
    if len(args) == 1:
        return args[0]
    L = list(args)
    while len(L) > 1:
        a, b = L[len(L) - 2], L[len(L) - 1]
        L = L[:len(L) - 2]
        while a:
            a, b = b % a, a
        L.append(b)
    return abs(b)

def normalize_list(l: list) -> list:
    if len(l) == 0:
        return l
    d = {x: l.count(x) for x in l}
    div = gcd(list(d.values()))
    norm = [[k] * (v // div) for k, v in d.items()]
    return [item for sublist in norm for item in sublist]

##############################################################
