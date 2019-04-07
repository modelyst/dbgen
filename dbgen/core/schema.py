 # External
from typing import (Any, TYPE_CHECKING,
                    Set      as S,
                    List     as L,
                    Dict     as D,
                    Union    as U,
                    Iterator as I)
from abc import ABCMeta,abstractmethod
# Internal
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
    from dbgen.core.query       import Query
    from dbgen.core.schemaclass import Schema
    Model,Query,Schema

from dbgen.core.sqltypes   import SQLType, Int
from dbgen.core.action     import Action
from dbgen.core.misc       import Dep
from dbgen.core.expr       import PathAttr
from dbgen.core.pathconstraint import Path as AP
from dbgen.core.funclike   import Arg, PyBlock
from dbgen.utils.misc      import Base
from dbgen.utils.sql       import (Connection as Conn,mkSelectCmd,sqlselect,
                                   mkInsCmd,sqlexecute)
########################################################################################
######################
# Simple Tuple types #
######################
class AttrTup(Base):
    def __init__(self, name : str, obj : str) -> None:
        self.name = name
        self.obj  = obj

    def __str__(self) -> str:
        return self.name + '.' + self.obj

    def __call__(self, x : AP = None) -> PathAttr:
        return PathAttr(x,self)

class RelTup(Base):
    def __init__(self, objname : str, relname : str) -> None:
        self.obj = objname.lower()
        self.rel = relname.lower()

    def __str__(self) -> str:
        return 'Rel(%s,%s)'%(self.obj, self.rel)

################################################################################
class Attr(Base):
    def __init__(self,
                 name    : str,
                 dtype   : SQLType = None,
                 id      : bool    = False,
                 default : Any     = None,
                 desc    : str     = '<No description>'
                 ) -> None:
        self.name    = name.lower()
        self.desc    = desc
        self.dtype   = dtype or Int()
        self.id      = id
        self.default = default

    def __str__(self) -> str:
        return 'Attr<%s,%s>'%(self.name, self.dtype)

    def create_col(self) -> str:
        dt   = str(self.dtype)
        dflt = '' if self.default is None else "DEFAULT %s"%(self.default)
        desc = "COMMENT '%s'"%self.desc.replace("'","''") if self.desc else ''
        fmt_args = [self.name,dt,dflt,desc]
        return "`{}` \t{} {} {}".format(*fmt_args)

class View(Base,metaclass=ABCMeta):
    def __str__(self) -> str:
        return 'View(%s)'%(self.name)
    name = ''
    @abstractmethod
    def dep(self) -> Dep:
        raise NotImplementedError
    @abstractmethod
    def qstr(self)->str:
        '''String representing the query (either JSON or raw SQL)'''
        raise NotImplementedError
    def add(self,cxn:Conn)->int:
        '''add view to metadb, return PK'''
        get_v = mkSelectCmd('view',['view_id'],['uid'])
        v_id  = sqlselect(cxn,get_v,[self.hash])

        if v_id:
            return v_id[0][0] # already there
        else:
            cmd  = mkInsCmd('view',['uid','name','query'])
            sqlexecute(cxn,cmd,[self.hash,self.name,self.qstr()])
            return sqlselect(cxn,get_v,[self.hash])[0][0]
class QView(View):
    def __init__(self, name : str,  q : 'Query' ) -> None:
        self.name = name
        self.q = q

    def qstr(self) -> str: return self.q.toJSON()

    def dep(self) -> Dep:
        cd = ['%s.%s'%(a.obj,a.name) for a in self.q.allattr()]
        nc = ['%s.%s'%(self.name,x) for x in self.q.exprs]
        return Dep(self.q.allobj(),cd,[self.name],nc)

class RawView(View):
    def __init__(self, name:str, q:str, deps : L[str] = None, new : L[str] = None) -> None:
        self.name = name
        self.raw  = q
        self.deps = deps or []
        self.new  = new or []

    def qstr(self) -> str: return self.raw

    def dep(self) -> Dep:
        td = [d for d in self.deps if '.' not in d]
        cd = [d for d in self.deps if '.' in d]
        nc = ['%s.%s'%(self.name,x) for x in self.new]
        return Dep(td,cd,[self.name],nc)

class Obj(Base):
    def __init__(self,
                 name  : str,
                 desc  : str     = None,
                 attrs : L[Attr] = None,
                 id    : str     = None
                ) -> None:

        self.name  = name.lower()
        self.desc  = desc  or '<No description>'
        self.attrs = {a.name : a for a in attrs or []}
        self._id   = id if id else self.name + '_id'
        # Validate
        self.forbidden = [self._id,'uid','deleted'] # RESERVED
        assert not any([a in self.forbidden for a in self.attrs])

    def __str__(self) -> str:
        return 'Object<%s, %d attrs>'%(self.name,len(self.attrs))

    def __call__(self,**kwargs : Any) -> Action:
        kwargs = {k.lower():v for k,v in kwargs.items()} # convert keys to L.C.
        pk_     = kwargs.pop(self.name, None)
        if pk_:
            assert isinstance(pk_,Arg), 'Invalid pk type: %s%s'%(pk_,type(pk_))
            pk = pk_.name
        else:
            pk = pk_
        insert = kwargs.pop('insert',False)
        if not pk: # if we don't have a PK reference
            assert all([i in kwargs for i in self.ids()])
        attrs = {k:v for k,v in kwargs.items() if k in self.attrnames()}
        fks   = {k:v for k,v in kwargs.items() if k not in attrs}
        for k,v in fks.items():
            if not isinstance(v,Action):
                # We OUGHT have a reference to a FK from a query
                assert isinstance(v,Arg)
                assert v.key == 'query', 'Is %s really a relation?'%k
                fks[k] = Action(obj = k, attrs = {}, fks = {}, pk = v.name)
        return Action(self.name, attrs = attrs, fks = fks, pk = pk, insert = insert)

    def __getitem__(self, key : str ) -> AttrTup:
        if key in self.attrs:
            return AttrTup(key,self.name)
        else:
            raise KeyError(key+' not found in %s'%self)

    # Public methods #

    def get(self, key: str) -> AttrTup:
        return AttrTup(key,self.name)

    def act(self, **kwargs : Any) -> Action:
        return self(**kwargs)

    def add_attrs(self,ats:L[Attr]) -> None:
        for a in ats:
            assert not a in self.attrs or a.name in self.forbidden
        self.attrs.update({a.name:a for a in ats})

    def del_attrs(self,ats:L[str])->None:
        for a in ats:
            del self.attrs[a]

    def r(self, relname : str) -> RelTup:
        return RelTup(self.name,relname)

    def attrnames(self,init : bool = False) -> L[str]:
        return [n for n,a in self.attrs.items() if a.id or not init]

    def create(self) -> str:
        create_str    = 'CREATE TABLE IF NOT EXISTS `%s` ' % self.name

        cols          = [a.create_col() for a in self.attrs.values()]

        pk    = self._id+' INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT'
        uid   = 'uid VARCHAR(255) NOT NULL UNIQUE'
        deld  = 'deleted TINYINT NOT NULL DEFAULT 0'
        cols  = [pk,uid,deld] + cols

        desc          = "COMMENT = '%s'"%self.desc.replace("'","''")

        fmt_args      = [create_str,'\n\t,'.join(cols),desc]
        cmd           = "{}\n\t({}) {}".format(*fmt_args)
        return cmd

    @property
    def id(self) -> AttrTup:
        '''Main use case: GROUP BY an object, rather than a particular column'''
        return AttrTup(self._id,self.name)

    def ids(self) -> L[str]:
        '''Names of all the identifying (top-level) attributes '''
        return [n for n,a in self.attrs.items() if a.id]

    def add(self, cxn : Conn) -> int:
        '''
        Add this Object to a metaDB that stores information about a model (if
        it's not already there), and return the ID.
        '''
        # Try to find an Object with an equivalent hash in the existing table
        get_t = mkSelectCmd('object',['object_id'],['uid'])
        t_id  = sqlselect(cxn,get_t,[self.hash])

        if t_id:
            return t_id[0][0] # already there
        else:
            # Create a new record in the Object table and get its ID
            name = self.name
            cmd  = mkInsCmd('object',['uid','name','description'])
            sqlexecute(cxn,cmd,[self.hash,name,self.desc])
            tab_id = sqlselect(cxn,get_t,[self.hash])[0][0]

            # Before returning ID, we have to populate Attr table
            ins_cols = ['object','attr_id','name','dtype','description',
                        'defaultval','uid']

            for col_id,c in enumerate(self.attrs.values()):
                # Insert info about an attribute
                binds = [tab_id,col_id,c.name,str(c.dtype),c.desc,
                          str(c.default),c.hash]

                cmd = mkInsCmd('attr',ins_cols)
                sqlexecute(cxn,cmd,binds)

            return tab_id

    def rename_attr(self,aname:str,newname:str)->'Obj':
        ''' Copy of object with a renamed attribute '''
        o = self.copy()
        newattr = o.attrs[aname]
        newattr.name = newname
        return o

    def default_action(self, pb : PyBlock) -> Action:
        raise NotImplementedError

    ###################
    # Private Methods #
    ###################
    #?


class Rel(Base):
    def __init__(self,
                 name : str,
                 o1   : str,
                 o2   : str = None,
                 id   : bool= False,
                 desc : str = '<No description>'
                ) -> None:
        self.name = name.lower()
        self.desc = desc
        self.o1   = o1.lower()
        self.o2   = o2.lower() if o2 else self.name
        self.id   = id

    def __str__(self) -> str:
        return 'Rel<%s%s,%s -> %s>'%(self.name,'(id)' if self.id else '',
                                      self.o1,self.o2)
    def __repr__(self) -> str:
        return '%s__%s'%(self.o1,self.name)
    # Public methods #

    def print(self) -> str:
        return '%s.%s'%(self.o1, self.name)

    def tup(self) -> RelTup:
        '''Throw away info to make a RelTup'''
        return RelTup(self.o1,self.name)

    @property
    def default(self)->bool:
        '''Whether or not this Relation is the 'default' one'''
        return self.name == self.o2

    @property
    def objs(self) -> S[str]:
        return set([self.o1, self.o2])

    def other(self, obj : str) -> str:
        '''
        Often we don't know which direction we are traversing on a FK
        We know which end we're coming from and simply want the other end
        '''
        assert obj in self.objs, '%s not found in %s'%(obj,self)

        out =  [o for o in self.objs if o != obj]

        if out: return out[0] # normal case
        else:   return obj    # both to and from are the same table!

    # Private Methods #
    #...

class Path(Base):
    def __init__(self,rels : L[RelTup] = None, attr : AttrTup = None) -> None:
        self.rels = rels or []
        self.attr  = attr
        assert rels or attr

    def __str__(self) -> str:
        p     = '[%s]'%','.join(map(str,self.rels)) if self.rels else ''
        comma = ',' if self.rels else ''
        a     = comma+str(self.attr) if self.attr else ''
        return 'Path(%s%s)'%(p,a)

    def start(self) -> str:
        '''Starting point of a path, always an object (name is returned)'''
        if self.rels:
            return self.rels[0].obj
        else:
            assert self.attr
            return self.attr.obj

    def select(self, m : 'Model') -> str:
        ''' Thing to select for when comparing path equality '''
        alias = self.start()
        for r in self.rels:
            alias += '$' + r.rel
        col = self._end_attr(m).name
        return '`%s`.`%s`'%(alias,col)

    def joins(self, ids : D[str,str], m: 'Model') -> L[str]:
        '''From clause that makes self.select() defined in query'''

        j  = [] # type: L[str]
        oldpath = self.start()
        jstr = 'JOIN `{0}` AS `{1}` ON `{1}`.`{4}` = `{2}`.`{3}`'
        next = self.start()
        for i,r in enumerate(self.rels[:-1]):
            newpath = oldpath + '$' + r.rel
            next    = self.rels[i+1].obj
            curr    = r.obj
            nextid  = ids[next]
            args    = [next, newpath, oldpath, r.rel, nextid]
            j.append(jstr.format(*args))
            oldpath = newpath
        if self.rels:
            last     = self.rels[-1].rel
            lastid   = ids[m.get_rel(self.rels[-1]).o2]
            newpath  = oldpath + '$' + last
            lastargs = [self._end_attr(m).obj,newpath,oldpath, last,lastid]
            j.append(jstr.format(*lastargs))
        return j

    def _end_attr(self, m : 'Model') -> AttrTup:
        '''the ID colname if we don't have a normal attribute as terminus'''
        if (not self.attr) :
            rel = m.get_rel(self.rels[-1])
            return m[rel.o2].id
        else:
            return self.attr

    def _path_end(self, m : 'Schema') -> str:
        '''Determine the datatype of the end of a path'''
        if (not self.attr) or (m[self.attr.obj].id == self.attr):
            return 'id'
        else:
            o = m[self.attr.obj]
            a = [a for n,a in o.attrs.items() if n == self.attr.name]
            return str(a[0].dtype)

class PathEQ(Base):
    def __init__(self, p1 : Path, p2 : Path) -> None:
        assert p1 != p2, 'Cannot do pathEQ between things that are literally equivalent'
        assert p1.start() == p2.start(), 'Paths must have same start point'
        self.paths = set([p1,p2])

    def __str__(self) ->str:
        return 'PathEQ({},{})'.format(*self.paths)

    def __iter__(self) -> I[Path]:
        return iter(self.paths)

    def __contains__(self, a : AttrTup) -> bool:
        if isinstance(a,AttrTup):
            return any([a == p.attr for p in self.paths])
        else:
            raise TypeError('add to this to support more types of searching')

    def any(self) -> Path:
        '''Gives one of the paths, doesn't matter which'''
        return next(iter(self))

    def start(self) -> str:
        return self.any().start()
