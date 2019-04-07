# External
from typing        import Any, Callable as C, Tuple as T, List as L
from unittest      import TestCase
from os            import environ

# Internal

from dbgen import (ConnectInfo as Conn, sqlselect, sqlexecute)
from dbgen.test.test_objects    import make_model
from dbgen.test.test_generators import addgens
from dbgen.example.main         import make_model as example
################################################################################

# Constants
kwargs = dict(nuke = True, retry = False, only = '', add = False,
              xclude = '', start = '', until = '', serial = False,
              bar = False) # for model.run()

connect_infos = [Conn.from_file(environ[x]) for x in ['TEST_DB','TEST_LOG']]
################################################################################

class IntegrationTest(TestCase):

    inspect = True # Manually inspect (in mySQLworkbench) before tearDown

    def setUp(self) -> None:
        self.db,self.mdb = connect_infos
        self.model = make_model()

        addgens(self.model)

        self.model.run(conn = self.db, meta_conn = self.mdb, **kwargs) # type: ignore

        cxn      = self.db.connect()
        self.sel : C[[str],L[T]] = lambda x: sqlselect(cxn,x)


    def test_bigbang(self)  ->  None:
        '''Any properties of the complete database we want to check?'''


    def tearDown(self)  ->  None:

        if self.inspect: import pdb;pdb.set_trace()
        for db in [self.db, self.mdb]: db.drop()


class ExampleTest(TestCase):
    inspect = True

    def setUp(self) -> None:
        self.db,self.mdb = connect_infos
        self.model = example()

        self.model.run(conn = self.db, meta_conn = self.mdb, **kwargs) # type: ignore

        cxn      = self.db.connect()
        self.sel = lambda x: sqlselect(cxn,x) # type: C[[str],L[T]]

    def test_bigbang(self)  ->  None:
        '''Any properties of the complete database we want to check?'''

    def tearDown(self)  ->  None:

        if self.inspect: import pdb;pdb.set_trace()
        for db in [self.db, self.mdb]: db.drop()
