from typing  import (Any, TypeVar,
                     List     as L,
                     Union    as U,
                     Dict     as D,
                     Callable as C,
                     Iterable)
from decimal  import Decimal
from datetime import datetime
##############################################################################
A = TypeVar('A'); B = TypeVar('B')
##############################################################
def flatten(lol: L[L[A]])->L[A]:
    """Convert list of lists to a single list via concatenation"""
    return [item for sublist in lol for item in sublist]
##############################################################
def nub(seq: L[A], idfun : C = None)->L[A]:
    """
    Remove duplicates but preserve order.
    """
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
    """
    Maps a function over an input.
    We apply the function to every element in the list and concatenate result.
    """
    return flatten([f(arg) for arg in args])

##############################################################
def broadcast(args : L[U[list,A]]) -> L[A]:
    """
    Enforce that all non-length-1 elements have the same length and replicate
    length-1 elements to the largest length list, then zip all the lists
    """
    valid_types = (int,str,tuple,float,list,bytes,datetime,Decimal,type(None))
    type_err    = "Arg (%s) BAD DATATYPE %s IN NAMESPACE "
    broad_err   = "Can't broadcast: maxlen = %d, len a = %d (%s)"
    maxlen = 1 # initialize variable

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
    # if maxlen == 1:
    #     import pdb;pdb.set_trace()
    #     return broadcasted
    # else:
    return list(zip(*broadcasted))
##############################################################
def batch(iterable : list, n : int = 1)->Iterable:
    """
    returns an iterable that where every iteration
    returns n of items from the original list
    """
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]
