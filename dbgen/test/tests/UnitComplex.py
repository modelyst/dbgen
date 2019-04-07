# External
from typing        import Any
from unittest      import TestCase
from unittest.mock import Mock
from re            import match
from os            import environ

# Internal
from dbgen import (ConnectInfo as Conn, sqlexecute, sqlselect)
from dbgen.test.test_objects    import make_model
from dbgen.core.schema          import Obj, Rel
from dbgen.core.query           import Query

'''
Any unit testing that requires Mocks
'''

################################################################################

# Constants
kwargs = dict(nuke = True, retry = False, only = set(), add = False,
              xclude = set(), start = '', until = '', serial = False,
              bar = False) # for model.run()

connect_infos = [Conn.from_file(environ[x]) for x in ['TEST_DB','TEST_LOG']]

################################################################################

class TestModel(TestCase):
    'RawView (mkView), Model(fk_graph,allparents,get) '

    def setUp(self) -> None:
        self.db,self.mdb = connect_infos
        self.model : Any = make_model()

    def test_same_db_error(self) -> None:
        '''
        Make sure an error is thrown if run() is passed two identical DBs
        '''
        with self.assertRaises(AssertionError):
            self.model.run(conn = self.db, meta_conn = self.db, **kwargs) # type: ignore



    def test_fk_graph(self) -> None:
        '''
        Verify a few FK relations in the test model exist in the FK graph
        '''
        G = self.model._fks

        # Parent relations
        self.assertIn(('rating','reviewer'), G.edges)


    def test_get(self) -> None:
        '''
        Test both means of accessing Objects in a model
        '''
        self.assertIs(self.model['Movie'], self.model.get('movie'))


class TestInsert(TestCase):
    '''Public methods: act() '''
    def test_act(self) -> None:
        pass

class TestObject(TestCase):
    '''select, randGen, add_cols, create_table (FK: show, flip, attr)'''

    def test_select(self) -> None:
        pass

    def test_add_cols(self) -> None:
        pass

    def test_create_table(self) -> None:
        pass


class TestGenerator(TestCase):
    '''Public methods: '''

    def test_update_status(self) -> None:
        pass

    def test_run(self) -> None:
        pass

    def test_hash(self) -> None:
        pass
