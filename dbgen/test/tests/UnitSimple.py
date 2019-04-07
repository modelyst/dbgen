# External
from unittest      import TestCase
from os            import environ
from re            import match

# Internal
from dbgen import (ConnectInfo as Conn)
from dbgen.core.func          import Func
from dbgen.core.schema        import Rel
from dbgen.core.fromclause    import From, Path
################################################################################

# Constants
kwargs = dict(nuke = True, retry = False, only = set(), add = False,
              xclude = set(), start = '', until = '', serial = False,
              bar = False) # for model.run()

connect_infos = [Conn.from_file(environ[x]) for x in ['TEST_DB','TEST_LOG']]

################################################################################

# Unit Tests for public methods



class TestFunc(TestCase):
    '''
    Public methods: apply, add
    '''
    def setUp(self) -> None:
        '''Create example function for testing'''
        def add(a:int,b:int)->int:
            'docstring'
            return a + b
        self.add = add

    def test_apply(self) -> None:
        '''Check application of function is preserved'''
        self.assertEqual(Func.from_callable(self.add)(1,2),3)
        self.assertEqual(Func.from_callable(lambda a,b: a+b)(1,2), 3)

    def test_get_source(self) -> None:
        src = r"def add\(a:int,b:int\)->int:\s+'docstring'\s+return a \+ b"
        assert match(src,Func.from_callable(self.add).src), 'No Match:\n%s\n\n%s'%(src,Func.from_callable(self.add).src)

    def test_add(self) -> None:
        '''Test adding Func to meta-db'''
        pass

class TestJoin(TestCase):
    def test_str(self) -> None:
        pass

class TestFrom(TestCase):
    '''
    Imagine the following situation, where we want attributes from four tables
    with information propagating from top left to right bottom (suppose the
    ):
                FK<'curr'>
    Adults     -------------->  Pet
    ^                           | | FK<'leastfav'>
    | FK<'parent'>              | | and FK<'mostused'>
    |                           V V
    Kids       ------------->   Toy -------> Country
                FK<'favorite'>     FK<origin>

    we want to generate the following SQL
    SELECT Country.president FROM
        Adults
            JOIN Kids AS parent_Kids ON parent_Kids.parent = Adults.id
            JOIN Pet  AS curr_Pet    ON Adults.curr = curr_Pet.id
            JOIN Toy  AS parent_favorite___curr_favorite__first_Toy
                            ON parent_Kids.favorite = p_..._Toy.id
                           AND curr_Pet.mostused    = p_..._Toy.id
                           AND curr_Pet.leastfav    = p_..._Toy.id
            JOIN Country AS parent_favorite___curr_favorite__first_T

    We have one basis element and 4 Joins
    J0 = ('adults',{})
    J1 = ('kids',{J0 : Edge_parent})
    J2 = ('pet', {J0 : Edge_curr})
    J3 = ('toy', {J1 : Edge_favorite,
                  J2 : Edge_pet})
    J4 = ('country',{J3 : Edge_origin})
    Solving this problem may have been necessitated if a query called for:
        Country.president(multi=[[Kids.Favorite],
                                 [Pet.Favorite,Pet.First]])


    Paths are [p,f,o] and [c,[l,m],o]
    after first pass we should have a structure like
    ('kids',[p])        --> [J1]
    ('toy',[p,f])       --> [J1,Join('toy',{J1:Edge_favorite})]
    ('country',[p,f,o]) --> [J1,J2,Join('country',{J2:Edge_})]

    second pass
    ('pet',[c])
    ('toy',[c,[l,m]])
    ('country',[c,[l,m],o])

    Combine?
    ('kids',   [[p]])
    ('pet',    [[c]])
    ('toy',    [[p,f],  [c,[l,m]]])
    ('country',[[p,f,o],[c,[l,m],o]])

    '''
    def setUp(self) -> None:
        self.from_ = From(['adults'])

    def test_add(self) -> None:

        p = Rel('parent',   'kids',  'adults')
        f = Rel('favorite', 'kids',  'toy')
        c = Rel('curr',     'adults','pet')
        l = Rel('leastfav', 'pet',   'toy')
        m = Rel('mostused', 'pet',   'toy')
        o = Rel('origin',   'toy',   'country')
        # paths = [Path([Edge([p]),Edge([f]),Edge([o])]),
        #          Path([Edge([c]),Edge([l,m]),Edge([o])])]
        # self.from_.add(Paths(paths))

class TestExpr(TestCase):
    '''ShowWhere, attrs/agg_attrs, -- attr(str,alias,create_col, path)'''

    def test_attrs(self) -> None:
        pass

    def test_agg_attrs(self) -> None:
        pass

    def test_attr_str(self) -> None:
        pass

    def test_attr_alias(self) -> None:
        pass

    def test_create_col(self) -> None:
        pass

    def test_path(self) -> None:
        pass
