# External Modules
from typing import (Any,
                    List     as L,
                    Dict     as D,
                    Tuple    as T,
                    Union    as U,
                    Callable as C)

from os                    import environ
from re                    import findall
from os.path               import join, exists
from sys                   import version_info
from inspect               import getdoc,signature,getsourcefile, getsourcelines,getmembers,isfunction # type: ignore
from importlib.util        import spec_from_file_location, module_from_spec
from hypothesis.strategies import SearchStrategy, builds # type: ignore

# Iternal Modules
from dbgen.core.datatypes   import DataType, Tuple
from dbgen.utils.misc  import hash_, Base
from dbgen.utils.sql   import (sqlexecute,mkInsCmd,sqlselect,mkSelectCmd,
                                Connection as Conn)

"""
Defines the Func class, which is initialized with any Python callable but gets
enriched in the __init__ with a lot more information and methods (from inspect)
"""

assert version_info[1] > 5, "Stop using old python3 (need 3.x, x > 5)"
################################################################################
class Import(Base):
    '''
    Representation of an Python import line.

    Examples:

    --> from libname import unaliased, things, aliased as Thing
        Import('libname',         <---- (DO NOT make this a keyword argument!!!)
                'unaliased',
                'things',
                aliased = 'Thing')

    --> import numpy as np
        Import('numpy', alias = 'np')

    Unsupported edge case: trying to import some variable literally named
                            "alias" using an alias
    '''
    def __init__(self,
                 lib              : str,
                 unaliased_terms  : L[str]=None,
                 alias            : str = '',
                 aliased_terms    : D[str,str] = None,
        ) -> None:

        err = "Can't import %s as %s AND import specific terms (%s,%s) at once"
        terms = unaliased_terms or aliased_terms
        assert not (alias and terms), err%(lib,alias,unaliased_terms,aliased_terms)

        self.lib              = lib
        self.alias            = alias
        self.unaliased_terms  = unaliased_terms or []
        self.aliased_terms    = aliased_terms or {}
        super().__init__()

    def __str__(self) -> str:
        if not (self.unaliased_terms or self.aliased_terms):
            alias = (' as %s' % self.alias) if self.alias else ''
            return 'import %s %s'%(self.lib, alias)
        else:
            als   = ['%s as %s'%(k,v) for k,v in self.aliased_terms.items()]
            terms = list(self.unaliased_terms) + als
            return 'from %s import %s'%(self.lib,', '.join(terms))

    def __eq__(self, other : object)->bool:
        return False if not isinstance(other,Import) else vars(self) == vars(other)

    def __hash__(self) -> int:
        return self.hash

    @classmethod
    def strat(cls) -> SearchStrategy:
        return builds(cls)

    @staticmethod
    def from_str(s : str) -> 'Import':
        '''Parse a header line (parens not supported yet)'''
        if s[:6] == 'import':
            if ' as ' in s[7:]:
                lib,alias = [s[7:].split('as')[i].strip() for i in range(2)]
                return Import(lib,alias=alias)
            return Import(s.split()[1].strip())
        else:
            i =  s.find('import')
            a,lib = s[:i].split()
            assert a == 'from', 'Bad source code beginning : \n\n\n'+s
            pat     = r'([a-zA-Z0-9\_]+\s*(?:as\s*[a-zA-Z0-9\_]+)?)'
            groups  = findall(pat,s[i+6:])
            objs    = [list(map(str.strip, g.split('as'))) for g in groups]
            objs_   = [x[0] if len(x) == 1 else tuple(x) for x in objs]
            unalias = [x[0] for x in objs if len(x) == 1]
            aliased = {x[0]:x[1] for x in objs if len(x) == 2}

            return Import(lib, unaliased_terms=unalias, aliased_terms=aliased)

class Env(Base):
    '''
    Environment in which a python statement gets executed
    '''
    def __init__(self, imports : L[Import] = None) -> None:
        self.imports = imports or []
        super().__init__()

    def __str__(self) -> str:
        return '\n'.join(map(str,self.imports))

    def __add__(self, other : 'Env') -> 'Env':
        return Env(list(set(self.imports + other.imports)))

    @classmethod
    def strat(cls) -> SearchStrategy:
        return builds(cls)

    # Public methods #

    @staticmethod
    def from_str(strs : L[str]) -> 'Env':
        '''Parse a header'''
        return Env([Import.from_str(s) for s in strs])

    @classmethod
    def from_file(cls,pth:str)->'Env':
        with open(pth,'r') as f:
            return cls.from_str(f.readlines())

defaultEnv = Env.from_file(environ['DEFAULT_ENV'])
emptyEnv   = Env()


################################################################################
class Func(Base):
    """
    A function that can be used during the DB generation process.
    """
    def __init__(self, src : str, env: Env = None) -> None:
        assert isinstance(src,str), 'Expected src str, but got %s'%type(src)

        self.src  = src

        if env:
            assert isinstance(env,Env), 'Expected Env, but got %s'%type(env)
            self.env  = env
        else:
            self.env  = Env.from_file(environ['DEFAULT_ENV'])
        super().__init__()


    def __str__(self) -> str:
        n = self.src.count('\n')
        s = '' if n == 1 else 's'
        return '<Func (%d line%s)>'%(n,s)

    def __call__(self,*args:Any) -> Any:
        if hasattr(self,'_func'):
            return self._func(*args) # type: ignore
        else:
            f = self._from_src()
            return f(*args)

    def __repr__(self) -> str: return self.name

    @classmethod
    def strat(cls) -> SearchStrategy:
        return builds(cls)

    # Properties #

    @property
    def name(self) -> str:
        return self._from_src().__name__

    @property
    def is_lam(self) -> bool:
        return self.src[:6] == 'lambda'

    @property
    def doc(self) -> str:
        return getdoc(self._from_src()) or ''

    @property
    def sig(self) -> Any:
        return signature(self._from_src()) # type: ignore

    @property
    def argnames(self) -> L[str]:
        return list(self.sig.parameters)

    @property
    def nIn(self) -> int:
        return len(self.inTypes)

    @property
    def notImp(self) -> bool:
        return 'NotImplementedError' in self.src

    @property
    def output(self) -> Any:
        return  self.sig.return_annotation

    @property
    def nOut(self) -> int:
        return len(self.outTypes)

    @property
    def inTypes(self) -> L[DataType]:
        return [DataType.get_datatype(x.annotation) for x in self.sig.parameters.values()]

    @property
    def outTypes(self) -> L[DataType]:
        ot = DataType.get_datatype(self.output)
        if len(ot) == 1:
            return [ot]
        else:
            assert isinstance(ot,Tuple)
            return ot.args

    def file(self) -> str:
        lam = 'f = ' if self.is_lam else ''
        return str(self.env)+'\n' + lam + self.src

    # Private methods #

    def _from_src(self) -> C:
        '''
        Execute source code to get a callable
        '''
        pth  = join(environ['DBGEN_TEMP'],str(hash_(self.file()))+'.py')

        if not exists(pth):
            with open(pth,'w') as t:
                t.write(self.file())

        f = self.path_to_func(pth)

        return f

    # Public methods #
    def store_func(self) -> None:
        '''Load func from source code and store as attribute (better performance
        but object is no longer serializable / comparable for equality )
        '''
        self._func = self._from_src()

    def del_func(self) -> None:
        '''Remove callable attribute after performance is no longer needed'''
        if hasattr(self,'_func'): del self._func

    def add(self, cxn : Conn) -> int:
        """
        Log function data to metaDB, return its ID
        """
        q    = mkSelectCmd('_func',['func_id'],['checksum'])
        f_id = sqlselect(cxn,q,[hash_(self.src)])
        if f_id:
            return f_id[0][0]
        else:

            cols = ['name','checksum','source','docstring',
                    'inTypes','outType','n_in','n_out']

            binds= [self.name,hash_(self.src),self.src,self.doc
                   ,str(self.inTypes),str(self.outTypes),self.nIn,self.nOut]

            sqlexecute(cxn,mkInsCmd('_func',cols),binds)
            f_id = sqlselect(cxn,q,[hash_(self.src)])
            return f_id[0][0]

    @staticmethod
    def path_to_func(pth : str) -> C:

        try:
            spec = spec_from_file_location('random',pth)
            mod  = module_from_spec(spec)
            assert spec and spec.loader, 'Spec or Spec.loader are broken'
            spec.loader.exec_module(mod) # type: ignore
            funcs = [o for o in getmembers(mod) if isfunction(o[1]) and getsourcefile(o[1])==pth]
            assert len(funcs)==1,"Bad input file %s has %d functions, not 1"%(pth,len(funcs))
            return funcs[0][1]

        except Exception as e:
            print('Error while trying to load source code', e, '\n\n'+pth);
            import pdb;pdb.set_trace()
            assert False, 'Error loading source code'

    @classmethod
    def from_callable(cls, f : U[C, 'Func'], env : Env = None) -> 'Func':
        """
        Generate a func from a variety of possible input data types.
        """
        if isinstance(f,Func):
            assert not getattr(env,'imports',False)
            return f
        else:
            assert callable(f), 'tried to instantiate Func, but not callable %s'%(type(f))
            return Func(src = cls.get_source(f), env=env)

    @staticmethod
    def get_source(f : C) -> str:
        """
        Return the source code, even if it's lambda function.
        """
        try:
            source_lines, _ = getsourcelines(f)
        except (IOError, TypeError) as e: # functions defined in pdb / REPL / eval / some other way in which source code not clear
            import pdb; pdb.set_trace()
            raise ValueError('from_callable: ',f,e)

        # Handle 'def'-ed functions and long lambdas
        src = ''.join(source_lines).strip()

        if len(source_lines) > 1 and src[:3]=='def':
            return src

        err = 'Only one "lambda" allowed per line: '
        assert src.count('lambda') == 1, err+src

        src_ = src[src.find('lambda'):] # start of lambda function

        # Slice off trailing chars until we get a callable function
        while len(src_) > 6:
            try:
                if callable(eval(src_)):
                    return src_
            except (SyntaxError,NameError): pass

            src_ = src_[:-1].strip()

        raise ValueError('could not parse lambda: '+src)
