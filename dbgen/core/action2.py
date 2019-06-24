# External Modules
from typing import (Any, TYPE_CHECKING,
                    List     as L,
                    Union    as U,
                    Dict     as D,
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
            self._insert(cxn=cxn,objs=objs,row=row)
        else:
            self._update(cxn=cxn,objs=objs,row=row)
        return None

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
    def _gethash(self,
                 objs : D[str,'Obj'],
                 row  : dict
                 ) -> L[str]:
        '''
        Get a broadcasted list of hashes for an object, given Pyblock+Query
        output
        '''

        if self.pk: # OPTION 1: we have a direct reference to the hash
            pk = self.pk.arg_get(row)
            if isinstance(pk,str): # this came from a Query in the form of: "PK HASH"
                return [pk.split()[1]] # take the second space-separated element
            elif isinstance(pk,list): # UNUSUAL: these values were provided manually
                for pk0,pk1 in pk:
                    assert isinstance(pk0,int) and isinstance(pk1,str)
                return [pk1 for _,pk1 in pk]
            else:
                raise TypeError(pk)
        else: # OPTION 2: we have data for all ID attrs and fks
            obj   = objs[self.obj]
            # Assemble all attribute info for the uid hash
            inits = [self.attrs[i].arg_get(row) for i in obj.ids()] + \
                    [self.fks[i]._gethash(objs,row) for i in obj.id_fks()]

            return [hash_(x) for x in broadcast(inits)]

    def _getvals(self,
                 objs : D[str,'Obj'],
                 cxn  : Conn,
                 row  : dict
                 ) -> L[list]:
        '''
        Get a broadcasted list of INSERT/UPDATE values for an object, given
        Pyblock+Query output
        '''
        obj = objs[self.obj]
        vals = [v.arg_get(row) for v in self.attrs.values()]
        for v in self.fks.values():
            if v.insert:
                vals.append(v._insert(objs,cxn,row))
            else:
                assert v.pk
                val = v.pk.arg_get(row)
                if isinstance(val,str): # came from query in form "PK HASH"
                    vals.append(val.split()[0])
                elif isinstance(val,list):# UNUSUAL: these values were provided manually
                    vals.append([pk0 for pk0,_ in val])
                else:
                    raise ValueError()
        return broadcast(vals)

    def _insert(self,
                objs : D[str,'Obj'],
                cxn  : Conn,
                row  : dict
               ) -> L[int]:
        '''
        Needs enough information to construct the uid of current object
        Needs the DB primary key and uids pointed to by ID FKs.
        '''

        obj = objs[self.obj]

        # Get hashes of current object using the ID ATTRIBUTES from all identifying relations
        uid = self._gethash(objs,row)


        gv = self._getvals(objs,cxn,row)
        if not gv:
            return []
        assert len(uid)==len(gv), '{}!={}'.format(len(uid),len(gv))
        binds = [list(x)+[u]+list(x) for x,u in zip(self._getvals(objs,cxn,row),uid)] # double the binds

        # Prepare insertion query
        #------------------------
        cols        = list(self.attrs.keys()) + list(self.fks.keys()) + ['uid']
        insert_cols = ','.join(['"%s"'%x for x in cols])
        qmarks      = ','.join(['%s']*len(cols))
        dups        = addQs(['"%s"'%x for x in cols[:-1]],', ')
        fmt_args    = [self.obj, insert_cols, qmarks, dups, obj._id]
        query       = """INSERT INTO {0} ({1}) VALUES ({2})
                         ON CONFLICT (uid) DO UPDATE SET {3}
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
        '''
        obj = objs[self.obj]
        # Validate
        for a in self.attrs:
            if a in obj.ids():
                raise ValueError('Cannot update an identifying attribute: ',a)
        for f in self.fks:
            if f in obj.id_fks():
                raise ValueError('Cannot update an identifying FK',f)

        assert self.pk is not None
        valstr = self.pk.arg_get(row) #row['query'][self.pk]
        assert isinstance(valstr,str), 'UPDATE PK SHOULD BE PROVIDED BY QUERY'
        val= valstr.split()[0]
        bindlists = self._getvals(objs,cxn,row)
        assert len(bindlists)==1
        binds = list(bindlists[0]) + [val]#[list(x)+[val] for x in bindlists]
        cols  = list(self.attrs.keys()) + list(self.fks.keys())
        set_  = addQs(['"%s"'%x for x in cols],',\n\t\t\t')
        objid = objs[self.obj]._id
        query = 'UPDATE {0} SET {1} WHERE {2} = %s'.format(self.obj,set_,objid)

        sqlexecute(cxn,query,binds)
