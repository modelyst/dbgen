# External Modules
from typing          import Any,List as L,Callable as C,TYPE_CHECKING, Tuple as T
from time            import sleep
from os              import environ
from os.path         import exists
from json            import load, dump
from pprint          import pformat
from MySQLdb         import connect,Connection,OperationalError # type: ignore
from MySQLdb.cursors import Cursor,SSDictCursor                 # type: ignore

# Internal Modules
if TYPE_CHECKING:
    from dbgen.core.gen import Gen
    Gen

from dbgen.utils.misc import Base
################################################################################
class ExternalError(Exception,Base):
    def __init__(self, message : str) -> None:
        super().__init__(message)
    def __str__(self) -> str:
        return super().__str__()
    # Add custom stuff here?
################################################################################

class ConnectInfo(Base):
    def __init__(self,
                 host   : str = '127.0.0.1',
                 port   : int = 3306,
                 user   : str = None,
                 passwd : str = None,
                 db     : str = ''
                ) -> None:

        if not user:
            user = passwd = environ["USER"]

        self.host   = host
        self.port   = port
        self.user   = user
        self.passwd = passwd
        self.db     = db

    def __str__(self) -> str:
        return pformat(self.__dict__)

    def connect(self, dic : bool = False , attempt : int  = 3) -> Connection:
        e = ''

        for _ in range(attempt):

            try:
                return connect(host        = self.host,
                               port        = self.port,
                               user        = self.user,
                               passwd      = self.passwd,
                               db          = self.db,
                               autocommit  = True,
                               cursorclass = SSDictCursor if dic else Cursor,
                               connect_timeout = 28800)
            except OperationalError as e:
                print(e)
                sleep(1)

        raise OperationalError()

    def to_file(self, pth : str) -> None:
        '''Store connectinfo data as a JSON file'''
        with open(pth,'w') as f:
            dump(vars(self),f)

    @staticmethod
    def from_file(pth : str) -> 'ConnectInfo':
        """
        Create from path to file with ConnectInfo fields in JSON format
        """
        assert exists(pth), 'Error loading connection info: no file at '+pth
        with open(pth,'r') as f:
            return ConnectInfo(**load(f))

################################################################################

class Dep(Base):
    def __init__(self,
                 tabs_needed  : L[str] = [],
                 cols_needed  : L[str] = [],
                 tabs_yielded : L[str] = [],
                 cols_yielded : L[str] = []
                 ) -> None:
        allts = [tabs_needed,tabs_yielded]
        allcs = [cols_needed,cols_yielded]
        assert all([all(['.' not in t for t in ts]) for ts in allts]), allts
        assert all([all(['.'     in c for c in cs]) for cs in allcs]), allcs
        self.tabs_needed  = set(tabs_needed)
        self.cols_needed  = set(cols_needed)
        self.tabs_yielded = set(tabs_yielded)
        self.cols_yielded = set(cols_yielded)

    def all(self) -> T[str,str,str,str]:
        a,b,c,d = tuple(map(lambda x: ','.join(sorted(x)),
                        [self.tabs_needed, self.cols_needed,
                        self.tabs_yielded, self.cols_yielded]))
        return a,b,c,d

    def __str__(self) -> str:
        return pformat(self.__dict__)

    def __bool__(self)->bool:
        return bool(self.tabs_needed | self.cols_needed | self.tabs_yielded | self.cols_yielded)
    # Public Methods #

    def test(self, other:'Dep') -> bool:
        '''Test whether SELF depends on OTHER'''
        return not (self.tabs_needed.isdisjoint(other.tabs_yielded) and
                    self.cols_needed.isdisjoint(other.cols_yielded))

    @classmethod
    def merge(cls, deps : L['Dep']) -> 'Dep':
        '''Combine a list of Deps using UNION'''
        tn,cn,ty,cy = set(),set(),set(),set() # type: ignore
        for d in deps:
            tn = tn | d.tabs_needed;  cn = cn | d.cols_needed
            ty = ty | d.tabs_yielded; cy = cy | d.cols_yielded
        return cls(tn,cn,ty,cy) # type: ignore
################################################################################


################################################################################
class Test(object):
    def __init__(self,
                 test    : C[['Gen',Any],bool],
                 message : C[[Any],str]
                ) -> None:
        self.test    = test
        self.message = message

    def __call__(self, t : 'Gen', *args : Any) -> Any:
        '''Run a test on a generator to see if it's supposed to be executed'''
        output = self.test(t, *args)
        return True if output else self.message(output)


#################
# Example Tests #
#################

onlyTest = Test(lambda t,o: (len(o) == 0) or (t.name in o) or any([g in t.tags for g in o]),# type: ignore
                lambda x: "Rule not in 'Only' input specification")

xTest = Test(lambda t,x:  (t.name not in x) and (not any([g in t.tags for g in x])), # type: ignore
             lambda x: "Excluded")
