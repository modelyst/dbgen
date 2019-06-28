# External Modules
from typing import (Any, TYPE_CHECKING,
                    List     as L,
                    Union    as U,
                    Dict     as D,
                    Tuple    as T,
                    Union    as U)
from collections import OrderedDict
from networkx    import DiGraph               # type: ignore

# Internal Modules
if TYPE_CHECKING:
    from dbgen.core.schema import Obj, Rel
    Obj,Rel

from dbgen.core.funclike import ArgLike,Arg
from dbgen.utils.misc   import hash_, Base
from dbgen.utils.lists  import broadcast
from dbgen.utils.sql    import (Connection as Conn, sqlselect, addQs,
                                 sqlexecute,sqlexecutemany)

'''
Defines the class of modifications to a database

There is a horrific amount of duplicated code in this file...... oughta fixit
'''
################################################################################

class Action(Base):
    """
    The purpose for this object is to make an easily serializable data structure
    that knows how to update the database (these methods could easily be for
    Model, but we don't want to send the entire model just to do this small thing)
    """
    def __init__(self,
                 obj    : str,
                 attrs  : D[str,ArgLike],
                 fks    : D[str,'Action'],
                 pk     : Arg    = None,
                 insert : bool   = False
                 ) -> None:

        self.obj    = obj.lower()
        self.attrs  = {k.lower():v for k,v in attrs.items()}
        self.fks    = {k.lower():v for k,v in fks.items()}
        self.pk     = pk
        self.insert = insert

        err = 'Cant insert %s if we already have PK %s'
        assert (pk is None) or (not insert), err%(obj,pk)
        assert isinstance(pk,(Arg,type(None))), (obj,attrs,fks,pk,insert)

    def __str__(self) -> str:
        n = len(self.attrs)
        m = len(self.fks)
        return 'Action<%s, %d attr, %d rel>'%(self.obj,n,m)

    ##################
    # Public methods #
    ###################
    def newtabs(self) -> L[str]:
        '''All tables that could be inserted into this action'''
        out = [self.obj] if self.insert else []
        for a in self.fks.values():
            out.extend(a.newtabs())
        return out

    def newcols(self) -> L[str]:
        '''All attributes that could be populated by this action'''
        out = [self.obj+'.'+a for a in self.attrs.keys()]
        for k,a in self.fks.items():
            out.extend([self.obj+'.'+k] + a.newcols())
        return out

    def act(self,
            cxn  : Conn,
            objs : D[str,'Obj'],
            row  : dict
           ) -> None:
        '''
        Top level call from a Generator to execute an action (top level is
        always insert or update, never just a select)
        '''
        if self.insert:
            self._insert(cxn,objs,row)
        else:
            self._update(cxn,objs,row)

    def rename_object(self, o : 'Obj', n :str) -> 'Action':
        '''Replaces all references to a given object to one having a new name'''
        a = self.copy()
        if a.obj == o.name:
            a.obj = n
        for k,v in a.fks.items():
            a.fks[k] = v.rename_object(o,n)
        return a

    ###################
    # Private methods #
    ###################

    def _getvals(self,
                 cxn  : Conn,
                 objs : D[str,'Obj'],
                 row  : dict,
                 ) -> T[L[int],L[list]]:
        '''
        Get a broadcasted list of INSERT/UPDATE values for an object, given
        Pyblock+Query output
        '''
        idattr,allattr = [],[]
        obj = objs[self.obj]
        for k,v in self.attrs.items():
            val = v.arg_get(row)
            allattr.append(val)
            if k in obj.ids():
                idattr.append(val)

        for kk,vv in self.fks.items():
            if vv.insert:
                val = vv._insert(cxn,objs,row)
            else:
                assert vv.pk
                val = vv.pk.arg_get(row)

            allattr.append(val)
            if kk in obj.id_fks():
                idattr.append(val)

        idata,adata = broadcast(idattr),broadcast(allattr)

        if self.pk is not None:
            assert not idata, 'Cannot provide a PK *and* identifying info'
            pkdata = self.pk.arg_get(row)
            if isinstance(pkdata,int):
                idata = [[pkdata]]
            elif isinstance(pkdata,list) and isinstance(pkdata[0],int):
                idata = [pkdata]
            else:
                raise TypeError('PK should either receive an int or a list of ints',vars(self))

        if len(idata) == 1: idata*= len(adata) # broadcast

        lenerr = 'Cannot match IDs to data: %d!=%d'
        assert len(idata) == len(adata), lenerr%(len(idata),len(adata))
        return list(map(hash_,idata)), adata

    def _insert(self,
                cxn  : Conn,
                objs : D[str,'Obj'],
                row  : dict,
               ) -> L[int]:
        '''
        Helpful docstring
        '''

        obj = objs[self.obj]
        pk,data = self._getvals(cxn,objs,row)
        if not data: return []

        binds = [list(x)+[u]+list(x) for x,u in zip(data,pk)] # double the binds

        # Prepare insertion query
        #------------------------
        cols        = list(self.attrs.keys()) + list(self.fks.keys()) + [obj._id]
        insert_cols = ','.join(['"%s"'%x for x in cols])
        qmarks      = ','.join(['%s']*len(cols))
        dups        = addQs(['"%s"'%x for x in cols[:-1]],', ')
        fmt_args    = [self.obj, insert_cols, qmarks, dups, obj._id]
        query       = """INSERT INTO {0} ({1}) VALUES ({2})
                         ON CONFLICT ({4}) DO UPDATE SET {3}
                         RETURNING {4}""".format(*fmt_args)

        ids = [sqlexecute(cxn,query,b)[0][0] for b in binds]
        return ids

    def _update(self,
                cxn  : Conn,
                objs : D[str,'Obj'],
                row  : dict
                ) -> None:
        '''
        Update but we don't have a PK
        # Can't this validation be done once before anything is run?
        # for a in self.attrs:
        #     if a in obj.ids():  raise ValueError('Cannot update an identifying attribute: ',a)
        # for f in self.fks:
        #     if f in obj.id_fks(): raise ValueError('Cannot update an identifying FK',f)
        '''
        obj = objs[self.obj]
        pk,data = self._getvals(cxn,objs,row)
        if not data: return None

        binds = [list(x)+[u] for x,u in zip(data,pk)]
        cols  = list(self.attrs.keys()) + list(self.fks.keys())
        set_  = addQs(['"%s"'%x for x in cols],',\n\t\t\t')
        objid = objs[self.obj]._id
        query = 'UPDATE {0} SET {1} WHERE {2} = %s'.format(self.obj,set_,objid)

        for b in binds: sqlexecute(cxn,query,b)
