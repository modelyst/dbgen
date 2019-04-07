# External Modules
from typing          import Any,List as L,Callable as C,TYPE_CHECKING, Tuple as T
from time            import sleep
from os              import environ
from os.path         import exists
from json            import load, dump
from pprint          import pformat
from psycopg2        import connect,Error                   # type: ignore
from psycopg2.extras import DictCursor                      # type: ignore
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # type: ignore
# Internal Modules
if TYPE_CHECKING:
    from dbgen.core.gen import Gen
    Gen

from dbgen.utils.misc import Base
"""
Defines some support classes used throughout the project:
- ExternalError
- ConnectInfo
- Test
- Dep

"""
Connection = Any
################################################################################
class ExternalError(Exception,Base):
    """
    Custom class for catching errors that occur in code external to dbgen
    """
    def __init__(self, message : str) -> None:
        super().__init__(message)
    def __str__(self) -> str:
        return super().__str__()
    # Add custom stuff here?
################################################################################

class ConnectInfo(Base):
    """
    PostGreSQL connection info
    """
    def __init__(self,
                 host   : str = '127.0.0.1',
                 port   : int = 5432,
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

    def connect(self, attempt : int  = 3) -> Connection:
        e = ''
        for _ in range(attempt):
            try:
                conn = connect(host        = self.host,
                               port        = self.port,
                               user        = self.user,
                               password    = self.passwd,
                               dbname      = self.db,
                               connect_timeout = 28800)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                return conn
            except Error as e:
                print(e)
                sleep(1)

        raise Error()

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

    def neutral(self)->Connection:
        copy = self.copy()
        copy.db = 'postgres'
        conn = copy.connect()
        return conn.cursor()

    def kill(self)->None:
        '''Kills connections to the DB'''
        killQ = '''SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = %s
                      AND pid <> pg_backend_pid();'''
        with self.neutral() as cxn:
            cxn.execute(killQ,vars=[self.db])

    def drop(self)->None:
        '''Completely removes a DB'''
        dropQ = 'DROP DATABASE IF EXISTS ' + self.db
        self.kill()
        with self.neutral() as cxn:
            cxn.execute(dropQ,vars=[self.db])

    def create(self)->None:
        '''Kills connections to the DB'''
        createQ = 'CREATE DATABASE ' + self.db
        with self.neutral() as cxn:
            cxn.execute(createQ,vars=[self.db])



################################################################################

class Dep(Base):
    '''
    Capture dependency information between two Generators that modify a DB
    through four different sets: the tabs/cols that are inputs/outputs.
    '''
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
    """
    Execute a test before running action. If it returns True, the test is
    passed, otherwise it returns an object which is fed into the "message"
    function. This prints a message: "Not Executed (<string of object>)"
    """
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
