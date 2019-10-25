from dbgen.core.schema import Obj, Attr, UserRel as Rel
from dbgen.core.schemaclass import Schema
from dbgen.core.action import Action
from dbgen.core.funclike import Arg
from dbgen.core.misc import ConnectInfo
from dbgen.core.expr.sqltypes import Varchar
from dbgen.utils.sql import sqlexecute, sqlselect

class TestAction:
    def setup(self):
        self.conn = ConnectInfo(db='_test')
        my_obj = Obj('my_obj', attrs=[Attr('my_attr',identifying=True)],
                     fks=[Rel('my_fk','other_obj')])
        other_obj = Obj('other_obj',attrs=[Attr('other_attr', Varchar(), identifying=True)])
        self.schema = Schema([my_obj, other_obj])
        self.objs = {oname : (o.id_str, o.ids(), o.id_fks())
                     for oname, o in self.schema.objs.items()}

        self.schema.make_schema(self.conn, nuke='true', bar=False)

        init = '''INSERT INTO other_obj (other_obj_id, other_attr) VALUES
                    (%s, %s), (%s, %s);'''

        sqlexecute(self.conn.connect(),init,[0, 'cat', 1, 'dog'])

    def test_insert(self):
        q = '''SELECT my_attr, other_attr FROM my_obj JOIN other_obj
                ON my_fk = other_obj_id ORDER BY my_attr'''
        assert not sqlselect(self.conn.connect(), q), 'Db is initially empty'
        my_fk = Action('other_obj',attrs={},fks={},pk=Arg('b','b1'))
        action = Action(obj='my_obj',
                        attrs=dict(my_attr=Arg('a','a1')),
                        fks=dict(my_fk=my_fk),
                        insert=True)
        rows = [dict(a=dict(a1=[1,2,3]), b=dict(b1=[0,1,0]))]

        action.act(self.conn.connect(), self.objs, rows)

        vals = sqlselect(self.conn.connect(), q)
        assert vals == [(1, 'cat'), (2, 'dog'), (3, 'cat')]
    def teardown(self):
        for x in ['my_obj','other_obj']:
            sqlexecute(self.conn.connect(), 'DROP TABLE IF EXISTS '+x)
