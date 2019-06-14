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

from dbgen.core.funclike import Arg, Const
from dbgen.utils.misc   import hash_, Base
from dbgen.utils.lists  import broadcast
from dbgen.utils.sql    import (Connection as Conn, sqlselect, addQs,
                                 sqlexecute)

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
                 attrs  : D[str,U[Arg,Const]],
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
            fks  : DiGraph,
            row  : dict
           ) -> None:
        '''
        Top level call from a Generator to execute an action (top level is
        always insert or update, never just a select)
        '''
        if self.insert:
            self._insert(cxn=cxn,objs=objs,fks=fks,row=row)
        else:
            self._update(cxn=cxn,objs=objs,fks=fks,row=row)
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

    def _select(self,
                cxn    : Conn,
                objs   : D[str,'Obj'],
                fks    : DiGraph,
                row    : D[str,Any],
                prefix : str = ''
               ) -> D[str,Any]:
        '''
        Add complementary information to dictionary containing information

        We either have a PK and want the underlying data of the instance or we
        have the data and want a PK

        Any information in self.attrs or self.fks that is NOT identifying info
        is completely disregarded for this task

        '''
        # Short-circuit if we have a Primary Key to work with
        if self.pk is not None:
            val = self.pk.arg_get(row) #row['query'][self.pk]
            if val is None:
                return {prefix +'.'+x:None for x in objs[self.obj].ids() }
            pre = prefix+'.'+self.obj+'.'
            return {pre+k:v for k,v in self._select_pk(cxn=cxn,objs=objs,fks=fks,prefix=pre,objstr=self.obj,val=val).items()}

        # Create Obj instance from the name of the object
        obj = objs[self.obj]

        # Dict top level attr names -> their values in the data dictionary "row"
        out = OrderedDict({(prefix + '.' + a) : arg.arg_get(row)
                            for a,arg in self.attrs.items()
                            if a in obj.ids()})

        # Recursively add to dict with parent attributes and their values
        for _,_,rels in sorted(fks.edges(self.obj,data=True)):
            # All identifying relations starting with the current object
            for rel in rels['fks']:
                if rel.id:
                    err = 'We should ALWAYS have ID relationships defined: '
                    assert rel.name in self.fks, err + str(rel.name)

                    # Get the identifying info of the subobject
                    new_dict = self.fks[rel.name]._select(cxn=cxn,objs=objs,fks=fks,row=row,prefix=prefix)

                    # Add it to current dict, DFS style
                    for k,v in new_dict.items():
                        out[prefix + '.' + rel.name + '.' + k] = v

        return out

    @classmethod
    def _select_pk(cls,
                   cxn    : Conn,
                   objs   : D[str,'Obj'],
                   fks    : DiGraph,
                   prefix : str,
                   objstr : str,
                   val    : int
                  ) -> D[str,Any]:
        '''
        We have a PK to some object and want an ordered dict of its ID attributes
        (recursively unpacked, DFS style)
        '''
        # Create Obj instance from the name of the object
        obj = objs[objstr]

        # Dict top level attr names -> their values in the data dictionary "row"
        fk_inits = []  # type: L[Rel]
        parents  = []  # type: L[Rel]

        edges = [x for _,_,x in sorted(fks.edges(objstr,data=True))]

        for edge in edges:
            # For every relation starting from current object
            for rel in edge['fks']:
                if rel.id:
                    fk_inits.append(rel)
                    parents.append(rel)

        colnames = obj.ids() + [f.name for f in fk_inits]

        if val is None:
            return {prefix+x:None for x in colnames}

        cols   = ','.join(['"%s"'%x for x in colnames])
        q      = 'SELECT {0} FROM {1} WHERE {2}=%s'.format(cols,objstr,obj._id)
        try:
            rawout = sqlselect(cxn,q,[val])[0]
        except IndexError:
            print(q,' failed!');import pdb;pdb.set_trace(); assert False
        out    = OrderedDict(zip(obj.ids(),rawout))
        nid    = len(obj.ids())
        for pkval,rel in zip(rawout[nid:],fk_inits):
            prefix_ = prefix+'.'+rel.name
            for k,v in cls._select_pk(cxn=cxn,objs=objs,fks=fks,
                                       prefix=prefix_,objstr=rel.o2,
                                       val = pkval).items():
                out[prefix_+'.'+k] = v

        return out


    def _insert(self,
                objs : D[str,'Obj'],
                fks  : DiGraph,
                cxn  : Conn,
                row  : dict
               ) -> None:
        '''
        We have enough information to either insert an instance of an Object
        or update it

        If we don't have a PK, we need to locate the record via its hash, which
        may involve invoking SELECT queries

        Consider A which has three identifying relationships (two to B, one to C)
        Let each object have an identifying attribute : a,b,c,d
        the idattrs are: ['a','r1.b','r1.r4.d','r2.b','r2.r4.d','r3.c']

           r1
          --->      r4
        A  r2   B  ---> D
          --->
          r3
          --> C

        '''
        if self.pk is not None:
            self._update(cxn=cxn,objs=objs,fks=fks,row=row)
        else:
            obj      = objs[self.obj]
            attriter = self.attrs.items()

            # Maintain a DICT of all ID attr vals (recursively expanded):
            initattr = OrderedDict({a:arg.arg_get(row) for a,arg in attriter
                                        if a in obj.ids()})

            # Maintain a DICT of all attrs/rels to be i/u (NOT recursive)
            attrs = {a:arg.arg_get(row) for a,arg in attriter}

            for _,_,rels in sorted(fks.edges(self.obj,data=True)):
                # For every relation starting from current object
                for rel in rels['fks']:
                    to_obj_id = objs[rel.o2]._id
                    # Stmt we will execute to get value for this FK
                    q = 'SELECT {0} FROM {1} WHERE uid = %s'.format(to_obj_id,rel.o2)

                    # List of (possibly many) values for this FK we will get
                    targ_ids = [] # type: L[int]

                    if rel.name in self.fks:

                        # Get the Action associated with this parent/component
                        new_act = self.fks[rel.name]
                        if new_act.pk is not None:
                            attrs[rel.name] = new_act.pk.arg_get(row) #row['query'][new_act.pk]

                            if rel.id:
                                # This would be nice, but is it really necessary? 
                                # errstr = 'Identifying FKs (e.g. %s.%s) should be populated from the QUERY'
                                # assert rel.name in row['query'], errstr%(self.obj,rel.name)
                                assert isinstance(attrs[rel.name],(int,type(None))), type(attrs[rel.name])
                                inits = new_act._select(cxn=cxn,objs=objs,fks=fks,row=row)
                        else:
                            # If this subaction is marked as "insert", then try to insert it first
                            if new_act.insert:
                                new_act._insert(cxn=cxn,objs=objs,fks=fks,row=row)

                            # Get Identifying information of target object {name:val}
                            inits = new_act._select(cxn=cxn,objs=objs,fks=fks,row=row)
                            # Get list of hash values for all instances of related object
                            hsh   = [hash_(x) for x in broadcast(inits,list(sorted(inits.keys())))]
                            for h in hsh: # Hash -> PK
                                try: targ_ids.append(sqlselect(cxn,q,[h])[0][0])
                                except IndexError:
                                    print(q); import pdb;pdb.set_trace(); assert False

                            # Store related object PKs under the relation name
                            attrs[rel.name] = targ_ids

                        # Only add to initattr if this is an identifying relation
                        if rel.id:
                            for k,v in inits.items():
                                initattr[rel.name+'.'+k] = v

            # Get hash(es) of current object using the ID ATTRIBUTES from all identifying relations
            attrs['uid']  = [hash_(x) for x in broadcast(initattr,list(sorted(initattr.keys())))]

            # Edge case, a "global" table w/ no inits
            if not attrs['uid']: attrs['uid'] = ''

            binds = broadcast(attrs,list(attrs.keys())*2)

            # Prepare insertion query
            #------------------------
            insert_cols = ','.join(['"%s"'%x for x in attrs])
            qmarks      = ','.join(['%s']*len(attrs))
            dups        = addQs(['"%s"'%x for x in attrs],', ')
            fmt_args    = [self.obj, insert_cols, qmarks, dups]
            query       = """INSERT INTO {0} ({1}) VALUES ({2})
                             ON CONFLICT (uid) DO UPDATE SET {3}""".format(*fmt_args)

            for b in binds:
                try: sqlexecute(cxn,query,b)
                except Exception as e:
                    print(e);print(query,b);import pdb;pdb.set_trace(); assert False


    def _update(self,
                cxn  : Conn,
                objs : D[str,'Obj'],
                fks  : DiGraph,
                row  : dict
                ) -> None:
        '''
        Update but we don't have a PK
        '''
        # Validate
        for a in self.attrs:
            if a in objs[self.obj].ids():
                raise ValueError('Cannot update an identifying attribute: ',a)

        # TODO: Also check that no FKs are IDs???

        # Call appropriate update function
        if self.pk is not None:
            self._update_from_pk(cxn=cxn,objs=objs,fks=fks,row=row)
        else:
            self._update_from_data(cxn=cxn,objs=objs,fks=fks,row=row)

    def _update_from_data(self,
                          cxn  : Conn,
                          objs : D[str,'Obj'],
                          fks  : DiGraph,
                          row  : dict
                         ) -> None:
        ''' Get hash and update row using that'''
        data = self._select(cxn=cxn,objs=objs,fks=fks,row=row)
        hsh  = [hash_(x) for x in broadcast(data,list(sorted(data.keys())))]

    def _update_from_pk(self,
                        cxn  : Conn,
                        objs : D[str,'Obj'],
                        fks  : DiGraph,
                        row  : dict
                        ) -> None:
        '''Special case of insertupdate where we have a PK - clearly we're not
        doing an insert. We can identify row with pk rather than hash
        '''
        assert self.pk is not None
        val = self.pk.arg_get(row) #row['query'][self.pk]

        # Maintain a DICT of all attrs/rels
        attrs = {a:arg.arg_get(row) for a,arg in self.attrs.items()}

        for _,_,rels in sorted(fks.edges(self.obj,data=True)):
            for rel in rels['fks']:
                if rel.name in self.fks:
                    new_act = self.fks[rel.name]
                    to_obj_id = objs[rel.o2]._id
                    # If this subaction is marked as "insert", then try to insert it first
                    if new_act.pk is not None:
                        targ = new_act.pk.arg_get(row) #row['query'][new_act.pk]
                    else:
                        if new_act.insert:
                            new_act._insert(cxn=cxn,objs=objs,fks=fks,row=row)

                        inits = new_act._select(cxn=cxn,objs=objs,fks=fks,row=row)
                        hsh   = [hash_(x) for x in broadcast(inits,list(sorted(inits.keys())))]
                        assert len(hsh) == 1
                        q = 'SELECT {0} FROM {1} WHERE uid = %s'.format(to_obj_id,rel.o2)
                        targ = sqlselect(cxn,q,[hsh[0]])[0][0]
                    attrs[rel.name] = targ

        assert attrs
        binds = list(attrs.values()) + [val]
        set_  = addQs(['"%s"'%x for x in attrs.keys()],',\n\t\t\t')
        objid = objs[self.obj]._id
        query = 'UPDATE {0} SET {1} WHERE {2} = %s'.format(self.obj,set_,objid)
        sqlexecute(cxn,query,binds)
        return None
