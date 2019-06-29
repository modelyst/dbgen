# Extenral
from typing     import (Any,
                        Callable as C,
                        List     as L,
                        Union    as U,
                        Dict     as D,
                        Tuple    as T)

from abc        import ABCMeta, abstractmethod
from traceback  import format_exc

# Internal
from dbgen.core.func   import Func, Env
from dbgen.core.misc   import ConnectInfo as Conn,ExternalError
from dbgen.utils.sql   import mkInsCmd,sqlexecute
from dbgen.utils.misc  import Base

'''
The "T" of ETL: things that are like functions.

A generalization of a function is a computational graph (where nodes are functions)

These nodes (PyBlocks) have Args which refer to other PyBlocks (or a query)
'''
####################################################################################
class ArgLike(Base,metaclass=ABCMeta):
    @abstractmethod
    def arg_get(self, dic : dict) -> Any:
        raise NotImplementedError

class Arg(ArgLike):
    """
    How a function refers to a namespace
    """

    def __init__(self, key : str, name  : str) -> None:
        self.key  = key.lower()
        self.name = name.lower()

    def __str__(self) -> str:
        return 'Arg(%s...,%s)'%(self.key[:4],self.name)

    # Public methods #

    def arg_get(self, dic : dict) -> Any:
        '''
        Common interface for Const and Arg to get values out of namespace
        '''
        try:
            val = dic[self.key][self.name]
            return val
        except KeyError as e:
            if self.key not in dic: print('could not find hash')
            else:
                err = 'could not find \'%s\' in %s '
                print(err %(self.name,list(dic[self.key].keys())))
            raise e

    def add(self, cxn : Conn, act : int, block : int) -> None:
        q = mkInsCmd('_arg',['gen_id','block_id','hashkey','name'])
        sqlexecute(cxn,q,[act,block,self.key,self.name])

class Const(ArgLike):
    def __init__(self,val:Any) -> None:
        if hasattr(val,'__call__'):
            try: val = Func.from_callable(val)
            except: pass
        self.val = val
    def __str__(self) -> str:
        return 'Const<%s>'%self.val
    def arg_get(self, _ : dict) -> Any:
        return self.val

class PyBlock(Base):
    """
    A computational block that executes Python code
    """
    def __init__(self,
                 func     : U[C,Func],
                 env      : Env                            = None,
                 args     : U[L[Arg],L[Const],L[ArgLike]]  = None,
                 outnames : L[str]                         = None,
                 tests    : L[T[tuple,Any]]                = None,
                ) -> None:
        # Store fields
        self.func     = Func.from_callable(func,env = env)
        self.args     = args or []
        self.outnames = [o.lower() for o in outnames or ['out']]
        self.tests    = tests or []

        # Validate input
        if outnames:
            dups = [o for o in outnames if outnames.count(o) > 1]
            err  = 'No duplicates in outnames for func %s: %s'
            assert not dups, err%(self.func.name, set(dups))

    def __str__(self)->str:
        return 'PyBlock<%d args>'%(len(self.args))

    def __lt__(self,other : "PyBlock") -> bool:
        return self.hash < other.hash

    def __call__(self, curr_dict : D[str,D[str,Any]]) -> D[str,Any]:
        """
        Take a TempFunc's function and wrap it such that it can accept a namespace
            dictionary. The information about how the function interacts with the
            namespace is contained in the TempFunc's args.
        """
        try:
            inputvars = [arg.arg_get(curr_dict) for arg in self.args]
        except (KeyError,TypeError,IndexError) as e:
            print(e)
            import pdb;pdb.set_trace()
            raise ValueError()

        try:
            output = self.func(*inputvars)
            if isinstance(output,tuple):
                l1,l2 = len(output),len(self.outnames)
                assert l1 == l2, 'Expected %d outputs from %s, got %d'%(l2,self.func.name,l1)
                return {o:val for val,o in zip(output,self.outnames)}
            else:
                assert len(self.outnames) == 1
                return {self.outnames[0] : output}
        except Exception as e:
            msg = '\tApplying func %s in tempfunc:\n\t'%(self.func.name)
            raise ExternalError(msg + format_exc())

    def __getitem__(self, key : str) -> Arg:
        err = '%s not found in %s'
        assert key.lower() in self.outnames, err %(key,self.outnames)
        return Arg(str(self.hash),key.lower())
    # Public Methods #

    def add(self,cxn:Conn,a_id:int,b_id:int)->None:
        '''Add to metaDB'''
        cols = ['gen_id','py_block_id','func_id','outnames']
        f_id = self.func.add(cxn)
        q    = mkInsCmd('_py_block',cols)
        sqlexecute(cxn,q,[a_id,b_id,f_id,','.join(self.outnames)])

    def test(self) -> None:
        '''Throw error if any tests are failed'''
        for args,target in self.tests:
            assert self.func(*args) == target
