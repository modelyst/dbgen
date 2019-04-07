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

################################################################################

class Action(Base):
    def __init__(self,
                 obj    : str,
                 attrs  : D[str,U[Arg,Const]],
                 fks    : D[str,'Action'],
                 pk     : str    = None,
                 insert : bool   = False
                 ) -> None:

        self.obj    = obj.lower()
        self.attrs  = {k.lower():v for k,v in attrs.items()}
        self.fks    = {k.lower():v for k,v in fks.items()}
        self.pk     = pk
        self.insert = insert


    def __str__(self) -> str:
        n = len(self.attrs)
        m = len(self.fks)
        return 'Action<%s, %d attr, %d rel>'%(self.obj,n,m)

    ##################
    # Public methods #
    ###################
    def newtabs(self) -> L[str]:
        out = [self.obj] if self.insert else []
        for a in self.fks.values():
            out.extend(a.newtabs())
        return out

    def newcols(self) -> L[str]:
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
        if self.insert:
            self._insert(cxn=cxn,objs=objs,fks=fks,row=row)
        else:
            self._update(cxn=cxn,objs=objs,fks=fks,row=row)
        return None

    def rename_object(self, o : 'Obj', n :str) -> 'Action':
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

        if self.pk:
            val = row['query'][self.pk]
            return self._select_pk(cxn=cxn,objs=objs,fks=fks,prefix=prefix,objstr=self.obj,val=val)

        obj = objs[self.obj]

        out = OrderedDict({(prefix + '.' + a) : arg.arg_get(row)
                            for a,arg in self.attrs.items()
                            if a in obj.ids()})

        for _,_,rels in sorted(fks.edges(self.obj,data=True)):
            for rel in rels['fks']:
                if rel.id:

                    new_dict = self.fks[rel.name]._select(cxn=cxn,objs=objs,fks=fks,row=row,prefix=prefix)

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
        obj = objs[objstr]

        fk_inits = []  # type: L[Rel]
        parents  = []  # type: L[Rel]

        edges = [x for _,_,x in sorted(fks.edges(objstr,data=True))]

        for edge in edges:
            for rel in edge['fks']:
                if rel.id:
                    fk_inits.append(rel)
                    parents.append(rel)

        colnames = obj.ids() + [f.name for f in fk_inits]

        cols   = ','.join(['`%s`'%x for x in colnames])
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
                out[k] = v

        return out


    def _insert(self,
                objs : D[str,'Obj'],
                fks  : DiGraph,
                cxn  : Conn,
                row  : dict
               ) -> None:
        if self.pk:
            self._update(cxn=cxn,objs=objs,fks=fks,row=row)
        else:
            obj      = objs[self.obj]
            attriter = self.attrs.items()

            initattr = OrderedDict({a:arg.arg_get(row) for a,arg in attriter
                                        if a in obj.ids()})

            attrs = {a:arg.arg_get(row) for a,arg in attriter}

            for _,_,rels in sorted(fks.edges(self.obj,data=True)):
                for rel in rels['fks']:
                    to_obj_id = objs[rel.o2]._id
                    q = 'SELECT {0} FROM {1} WHERE uid = %s'.format(to_obj_id,rel.o2)
                    targ_ids = [] # type: L[int]

                    if rel.name in self.fks:

                        new_act = self.fks[rel.name]
                        if new_act.pk:
                            attrs[rel.name] = row['query'][new_act.pk]
                            if rel.id:
                                inits = new_act._select(cxn=cxn,objs=objs,fks=fks,row=row)
                        else:
                            if new_act.insert:
                                new_act._insert(cxn=cxn,objs=objs,fks=fks,row=row)

                            inits = new_act._select(cxn=cxn,objs=objs,fks=fks,row=row)

                            hsh   = [hash_(x) for x in broadcast(inits,list(sorted(inits.keys())))]

                            for h in hsh: # Hash -> PK
                                try: targ_ids.append(sqlselect(cxn,q,[h])[0][0])
                                except IndexError:
                                    print(q); import pdb;pdb.set_trace(); assert False

                            attrs[rel.name] = targ_ids

                        if rel.id:
                            for k,v in inits.items():
                                initattr[rel.name+'.'+k] = v

            attrs['uid']  = [hash_(x) for x in broadcast(initattr,list(sorted(initattr.keys())))]

            if not attrs['uid']:
                attrs['uid'] = ''

            binds = broadcast(attrs,list(attrs.keys())*2)
            insert_cols = ','.join(['`%s`'%x for x in attrs])
            qmarks      = ','.join(['%s']*len(attrs))
            dups        = addQs(['`%s`'%x for x in attrs],', ')
            fmt_args    = [self.obj, insert_cols, qmarks, dups]
            query       = """INSERT INTO {0} ({1}) VALUES ({2})
                             ON DUPLICATE KEY UPDATE {3}""".format(*fmt_args)

            for b in binds:
                try: sqlexecute(cxn,query,b)
                except:
                    print(query,b);import pdb;pdb.set_trace(); assert False

    def _update(self,
                cxn  : Conn,
                objs : D[str,'Obj'],
                fks  : DiGraph,
                row  : dict
                ) -> None:
        for a in self.attrs:
            if a in objs[self.obj].ids():
                raise ValueError('Cannot update an identifying attribute: ',a)
        if self.pk:
            self._update_from_pk(cxn=cxn,objs=objs,fks=fks,row=row)
        else:
            self._update_from_data(cxn=cxn,objs=objs,fks=fks,row=row)

    def _update_from_data(self,
                          cxn  : Conn,
                          objs : D[str,'Obj'],
                          fks  : DiGraph,
                          row  : dict
                         ) -> None:
        data = self._select(cxn=cxn,objs=objs,fks=fks,row=row)
        hsh  = [hash_(x) for x in broadcast(data,list(sorted(data.keys())))]

    def _update_from_pk(self,
                        cxn  : Conn,
                        objs : D[str,'Obj'],
                        fks  : DiGraph,
                        row  : dict
                        ) -> None:
        val = row['query'][self.pk]

        attrs = {a:arg.arg_get(row) for a,arg in self.attrs.items()}

        for _,_,rels in sorted(fks.edges(self.obj,data=True)):
            for rel in rels['fks']:
                if rel.name in self.fks:
                    new_act = self.fks[rel.name]
                    to_obj_id = objs[rel.o2]._id
                    if new_act.pk:
                        targ = row['query'][new_act.pk]
                    else:
                        if new_act.insert:
                            new_act._insert(cxn=cxn,objs=objs,fks=fks,row=row)

                        inits = new_act._select(cxn=cxn,objs=objs,fks=fks,row=row)
                        hsh   = [hash_(x) for x in broadcast(inits,list(sorted(inits.keys())))]
                        q = 'SELECT {0} FROM {1} WHERE uid = %s'.format(to_obj_id,rel.o2)
                        targ = sqlselect(cxn,q,[hsh[0]])[0][0]
                    attrs[rel.name] = targ

        binds = list(attrs.values()) + [val]
        set_  = addQs(['`%s`'%x for x in attrs.keys()],',\n\t\t\t')
        objid = objs[self.obj]._id
        query = 'UPDATE {0} SET {1} WHERE {2} = %s'.format(self.obj,set_,objid)
        sqlexecute(cxn,query,binds)
        return None
