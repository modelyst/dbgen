# External
from typing import (Any, TYPE_CHECKING,
                    Set      as S,
                    List     as L,
                    Dict     as D,
                    Union    as U,
                    Tuple    as T,
                    Callable as C)

# Internal
if TYPE_CHECKING:
    from dbgen.core.schemaclass import Schema
    Schema

from dbgen.core.fromclause import From
from dbgen.core.expr       import Expr, PathAttr, Agg, One
from dbgen.core.funclike   import Arg
from dbgen.core.pathconstraint import Path
from dbgen.core.schema       import RelTup, Obj
from dbgen.core.misc       import ConnectInfo as ConnI
from dbgen.utils.lists     import flatten, nub
from dbgen.utils.sql        import select_dict

Fn = C[[Any],str] # type shortcut

################################################################################
class Query(Expr):
    def __init__(self,
                 exprs    : D[str,Expr],
                 basis    : L[U[str,Obj]] = None,
                 constr   : Expr          = None,
                 aggcols  : L[PathAttr]   = None,
                 aconstr  : Expr          = None,
                 option   : L[RelTup]     = None,
                 opt_attr : L[PathAttr]   = None,
                ) -> None:
        self.exprs   = exprs
        self.aggcols = aggcols  or []
        self.constr  = constr   or One
        self.aconstr = aconstr  or None

        if not basis:
            attrobjs = nub([a.obj for a in self.allattr()],str)
            assert len(attrobjs) == 1, 'Cannot guess basis for you %s'%attrobjs
            self.basis = attrobjs
        else:
            self.basis   = [x if isinstance(x,str) else x.name for x in basis]

        self.option   = option   or []
        self.opt_attr = opt_attr or []

    def __str__(self) -> str:
        return 'Query<%d exprs>'%(len(self.exprs))

    def __getitem__(self, key : str)->Arg:
        err = '%s not found in query exprs %s'
        assert key in self.exprs, err%(key,self.exprs)
        return Arg('query',key)

    ####################
    # Abstract methods #
    ####################
    def show(self, f : Fn) -> str:
        raise NotImplementedError

    def fields(self) -> L[Expr]:
        return list(self.exprs.values())
    ####################
    # Public methods #
    ####################

    def allobj(self) -> L[str]:
        return  [o.obj for o in self.option] \
              + self.basis + [a.obj for a in self.allattr()]

    def allattr(self) -> S[PathAttr]:
        es = list(self.exprs.values()) + [self.constr ,self.aconstr or One]
        return set(flatten([expr.attrs() for expr in es])
                   + self.aggcols)

    def allrels(self) -> S[RelTup]:
        out = set(self.option)
        for a in self.allattr():
            out = out | a.allrels()
        return out

    ###################
    # Private methods #
    ###################
    def _make_from(self) -> From:
        f     = From(self.basis)
        attrs = self.allattr()
        for a in attrs: f = f | a.path._from()
        return f

    def showQ(self) -> str:
        f = self._make_from()
        cols   = ',\n\t'.join(['%s AS `%s`'%(e,k) # .show(shower)
                                for k,e in self.exprs.items()])
        cols = ','+cols if cols else ''
        where  = str(self.constr) #self.show_constr({})
        notdel = '\n\t'.join(['AND NOT `%s`.deleted'%o for o in f.aliases()])
        notnul = '\n\t'.join(['AND %s IS NOT NULL'%(x)
                    for x in self.allattr() if x not in self.opt_attr])
        consts = 'WHERE %s' %('\n\t'.join([where,notdel,notnul]))
        if self.aconstr:
            haves  = '\nHAVING %s'%self.aconstr
        else:
            haves = ''
        gb      = [str(x) for x in self.aggcols]
        groupby  = '\nGROUP BY %s'%(','.join(gb)) if gb else ''
        f_str    = f.print(self.option)
        fmt_args = [f.pks(),cols,f_str,consts,groupby,haves]
        output = 'SELECT {0}\n\t{1}\n{2}\n{3}{4}{5}'.format(*fmt_args)
        return output

    def exec_query(self, db : ConnI) -> L[dict]:
        return select_dict(db, self.showQ())
