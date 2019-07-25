# External Modules
from abc    import abstractmethod,ABCMeta
from typing import (Any, TYPE_CHECKING,
                    Set      as S,
                    Dict     as D,
                    List     as L,
                    Tuple    as T,
                    Callable as C,
                    Optional as O)

from infix  import or_infix as pipe_infix # type: ignore
from copy   import deepcopy
from functools  import reduce
from operator   import add

# Internal Modules
if TYPE_CHECKING:
    from dbgen.core.schema import AttrTup,RelTup

from dbgen.core.pathconstraint  import Path
from dbgen.core.sqltypes        import SQLType,Decimal,Varchar,Text,Int
from dbgen.utils.lists          import concat_map
from dbgen.utils.misc           import Base
from dbgen.utils.lists          import flatten

"""
Python-sql interface
"""
###############################################################################
Fn = C[[Any],str] # type shortcut

class Expr(Base,metaclass = ABCMeta):

    # Constants
    #----------
    @property
    def agg(self)->bool: return False # by default, we assume not an Aggregation

    # Abstract methods
    #-----------------
    @abstractmethod
    def fields(self)->list:
        """
        List of immediate substructures of the expression (not recursive)
        """
        raise NotImplementedError

    @abstractmethod
    def show(self,f:Fn)->str:
        """
        Apply function recursively to fields
        """
        raise NotImplementedError

    #--------------------------#
    # Representing expressions #
    #--------------------------#
    def __str__(self)->str:
        """
        Default string representation: all fields run through str()
        """
        return self.show(lambda x:str(x))

    def __repr__(self)->str:
        return 'Expr<%s>'%(str(self))

    def __hash__(self) -> int:
        return hash(str(self))

    #--------------------#
    # Overloaded methods #
    #--------------------#
    def __abs__(self)->'ABS':              return ABS(self)
    def __add__(self, other:'Expr')->'PLUS':  return PLUS(self,other)
    def __mul__(self, other:'Expr')->'MUL':   return MUL(self,other)
    def __pow__(self, other:'Expr')->'POW':   return POW(self,other)
    def __sub__(self, other:'Expr')->'MINUS': return MINUS(self,other)
    def __or__(self,other : Any)->'Expr':     raise NotImplementedError
    del __or__  # tricking the type checker to use |Infix|
    def __truediv__(self, other:'Expr')->'DIV':   return DIV(self,other)

    #----------------#
    # Public methods #
    #----------------#

    def attrs(self)->L['PathAttr']:
        """
        Recursively search for any Path (Expr) mentions in the Expr
        """
        out = [] # type: L['PathAttr']
        for field in self.fields():
            if isinstance(field,PathAttr):
                out.append(field)
            elif hasattr(field,'attrs'):
                out.extend(field.attrs())

        return out

    #-----------------#
    # Private Methods #
    #-----------------#

    def _all(self)->list:
        """
        List of all fundamental things that are involved in the expression
        Recursively expand substructures and flatten result
        """
        return concat_map(self.get_all,self.fields())

    # Static methods #

    @staticmethod
    def get_all(x : Any) -> list:
        """If it has any recursive structure to unpack, unpack it"""
        return x._all() if hasattr(x,'_all') else [x]

################################################################################

##############
# Subclasses #
##############

class Unary(Expr):
    """
    Expression that depends on just one individual thing
    """
    # Input-indepedent parameters
    #----------------------------
    @property
    @abstractmethod
    def name(self)->str: raise NotImplementedError

    # Implement Expr abstract methods
    #--------------------------------
    def fields(self)->list:
        return [self.x]

    def show(self,f:Fn)->str:
        x = f(self.x)
        return '%s(%s)' % (self.name,x)

    # Class-specific init
    #-------------------
    def __init__(self,x:Expr)->None:
        assert isinstance(x,Expr)
        self.x = x

class Binary(Expr):
    """
    Expression that depends on two individual things
    """

    # Input-indepedent parameters
    #----------------------------
    @property
    @abstractmethod
    def name(self) -> str: raise NotImplementedError

    infix = True

    # Implement Expr abstract methods
    #--------------------------------
    def fields(self) -> list:
        return [self.x,self.y]

    def show(self, f : Fn) -> str:
        x,y = f(self.x), f(self.y)
        if self.infix:
            return '(%s %s %s)'%(x,self.name,y)
        else:
            return '%s(%s,%s)'%(self.name,x,y)

    # Class-specific init
    #-------------------
    def __init__(self,x:Expr,y:Expr)->None:
        assert all([isinstance(a,Expr) for a in [x,y]]), [x,type(x),y,type(y)]
        self.x,self.y = x,y

class Ternary(Expr):
    """
    Expression that depends on three individual things
    """
    # Input-indepedent parameters
    #----------------------------
    @property
    @abstractmethod
    def name(self) -> str: raise NotImplementedError
    # Implement Expr abstract methods
    #--------------------------------
    def fields(self) -> list:
        return [self.x,self.y,self.z]

    def show(self, f : Fn) -> str:
        x,y,z = f(self.x), f(self.y), f(self.z)
        return '%s(%s,%s,%s)'%(self.name,x,y,z)

    # Class-specific init
    #-------------------
    def __init__(self,x:Expr,y:Expr,z:Expr)->None:
        assert all([isinstance(a,Expr) for a in [x,y,z]])
        self.x,self.y,self.z = x,y,z

class Nary(Expr):
    """
    SQL Functions that take multiple arguments, initialized by user with
    multiple inputs (i.e. not a single list input)
    """
    # Input-indepedent parameters
    #----------------------------
    @property
    @abstractmethod
    def name(self) -> str: raise NotImplementedError

    @property
    def delim(self) -> str: return ',' # default delimiter

    def fields(self) -> list:
        return self.xs

    def __init__(self, *xs : Expr) -> None:
        self.xs  = list(xs)

    def show(self, f : Fn) -> str:
        xs = map(f,self.xs)
        d  = ' %s '%self.delim
        return '%s(%s)'%(self.name,d.join(xs))

class Named(Expr):
    """
    Inherit from this to allow any arbitrary class, e.g. XYZ(object),
     to automatically deduce that its 'name' property should be 'XYZ'
    """
    @property
    def name(self)->str:
        return type(self).__name__

class Agg(Expr):
    """
    This class is meant to be inherited by any SQL function we want to flag
    as an aggregation.

    We can optionally specify what objects we want to aggregate over, otherwise
    the intent will be guessed
    """
    @property
    def agg(self) -> bool: return True

    def __init__(self, x : Expr, objs : list = None) -> None:
        assert issubclass(type(x),Expr)
        self.x    = x
        self.objs = objs or []



################################################################################
# Specific Expr classes for user interface
###########################################

# Ones that work out of the box
#------------------------------
class ABS(Named,Unary):     pass
class SQRT(Named,Unary):    pass
class MAX(Named,Agg,Unary): pass
class SUM(Named,Agg,Unary): pass
class MIN(Named,Agg,Unary): pass
class AVG(Named,Agg,Unary): pass
class COUNT(Agg,Named,Unary): pass
class CONCAT(Named,Nary):     pass
class BINARY(Named,Unary):    pass  # haha
class REGEXP(Named,Binary):   pass
class REPLACE(Named,Ternary): pass
class COALESCE(Named,Nary):   pass


# @pipe_infix
class LIKE(Named,Binary):   pass

# Ones that need a field defined
#-------------------------------
class Tup(Nary):      name = ''
class LEN(Unary):     name = 'CHAR_LENGTH'
class MUL(Binary):    name = '*'
class DIV(Binary):    name = '/'
class PLUS(Binary):   name = '+'
class MINUS(Binary):  name = '-'
class POW(Named,Binary):          infix = False
class LEFT(Named,Binary):         infix = False
class RIGHT(Named,Binary):        infix = False
class JSON_EXTRACT(Named,Binary): infix = False

# @pipe_infix
class EQ(Binary): name = '='
# @pipe_infix
class NE(Binary): name = '!='
# @pipe_infix
class LT(Binary): name = '<'
# @pipe_infix
class GT(Binary): name = '>'
# @pipe_infix
class LE(Binary): name = '<='
# @pipe_infix
class GE(Binary): name = '>='

# @pipe_infix
class OR(Nary):
    """ Can be used as a binary operator (|OR|) or as a function OR(a,b,...)"""
    name  = ''
    delim = 'OR'

# @pipe_infix
class AND(Nary):
    name  = ''
    delim = 'AND'

class And(Nary):
    name  = ''
    delim = 'AND'

class NOT(Named,Unary):
    wrap = False

class NULL(Named,Unary):
    def show(self, f : Fn) -> str:
        return "%s is NULL"%f(self.x)

# Ones that need to be implemented from scratch
#----------------------------------------------

class Literal(Expr):
    def __init__(self,x : Any)->None:
        self.x = x

    def fields(self)-> L[Expr]: return []

    def show(self,f:Fn)->str:

        if isinstance(self.x,str):
            return "('%s')"%f(self.x).replace("'","\\'").replace('%','%%')
        elif self.x is None:
            return '(NULL)'
        else:
            x = f(self.x)
            return '(%s)' % x

# @pipe_infix
class IN(Named):
    def __init__(self,x:Expr,xs:L[Expr])->None:
        self.x   = x
        self.xs  = xs

    def fields(self)->L[Expr]:
        return [self.x]+self.xs

    def show(self,f:Fn)->str:
        xs = map(f,self.xs)
        return '%s IN (%s)'%(f(self.x),','.join(xs))

##########
class CASE(Expr):
    def __init__(self,cases:D[Expr,Expr],else_:Expr)->None:
        self.cases = cases
        self.else_  = else_
    def fields(self)->L[Expr]:
        return list(self.cases.keys())+list(self.cases.values())+[self.else_]
    def show(self,f:Fn)->str:
        body = ' '.join(['WHEN (%s) THEN (%s)'%(f(k),f(v))
                        for k,v in self.cases.items()])
        end = ' ELSE (%s) END'%(f(self.else_))
        return 'CASE  '+body + end


# IF_ELSE is *not* for public use; rather:  <Ex1> |IF| <Ex2> |ELSE| <Ex3>
class IF_ELSE(Expr):
    def __init__(self,cond:Expr,_if:Expr,other:Expr)->None:
        self.cond = cond
        self._if = _if
        self._else = other

    def fields(self)->L[Expr]:
        return [self.cond,self._if,self._else]
    def show(self,f:Fn)->str:
        c,i,e = map(f,self.fields())
        return 'CASE WHEN (%s) THEN (%s) ELSE (%s) END'%(c,i,e)

# @pipe_infix
def IF(outcome:Expr,condition:Expr)->T[Expr,Expr]:
    return (outcome,condition)

# @pipe_infix
def ELSE(ifpair : T[Expr,Expr], other : Expr) -> IF_ELSE:
    return IF_ELSE(ifpair[1],ifpair[0],other)

class CONVERT(Expr):
    def __init__(self, expr : Expr, dtype : SQLType) -> None:
        self.expr = expr
        self.dtype = dtype

        err = 'Are you SURE that Postgres can convert to this dtype? %s'
        assert isinstance(dtype,(Decimal,Varchar,Text,Int)), err%dtype

    def fields(self) -> L[Expr]:
        return [self.expr]

    def show(self,f:Fn) -> str:
        e = f(self.expr)
        return 'CAST(%s AS %s)'%(e,self.dtype)

class SUBSELECT(Expr):
    '''Hacky way of getting in subselect .... will not automatically detect
        dependencies'''
    def __init__(self,expr : Expr, tab : str, where : str = '1') -> None:
        self.expr = expr
        self.tab  = tab
        self.where= where

    def fields(self) -> L[Expr]:
        return [self.expr]

    def show(self,f:Fn) -> str:
        e = f(self.expr)
        return '(SELECT %s FROM %s WHERE %s )'%(e,self.tab,self.where)

class GROUP_CONCAT(Agg):
    def __init__(self,expr : Expr, delim : str = None, order : Expr = None) -> None:
        self.expr  = expr
        self.delim = delim or ','
        self.order = order

    def fields(self) -> L[Expr]:
        return [self.expr] + ([self.order] if self.order is not None else [])

    @property
    def name(self)->str: return 'string_agg'
    def show(self,f:Fn) -> str:
        ord = 'ORDER BY '+f(self.order) if self.order is not None else ''

        return 'string_agg(%s :: TEXT,\'%s\' %s)'%(f(self.expr),self.delim,ord)

class STD(Agg,Unary):
    name = 'stddev_pop'

##############################################################################
class PathAttr(Expr):
    def __init__(self, path : Path = None, attr : 'AttrTup' = None) -> None:
        assert attr
        self.path  = path or Path(attr.obj)
        self.attr  = attr

    def __str__(self) -> str:
        return '"%s"."%s"'%(self.path,self.attr.name)

    def __repr__(self) -> str:
        return 'Attr<%s.%s>'%(self.path,self.attr)

    ####################
    # Abstract methods #
    ####################
    def attrs(self) -> L['PathAttr']:
        return [self]

    def fields(self)->list:
        """
        List of immediate substructures of the expression (not recursive)
        """
        return []

    def show(self, f : Fn) -> str:
        """
        Apply function recursively to fields
        """
        return f(self)

    @property
    def name(self) -> str: return self.attr.name

    @property
    def obj(self)  -> str: return self.attr.obj

    def allrels(self) -> S['RelTup']:
        stack = list(self.path.fks)
        rels  = set()
        while stack:
            curr = stack.pop(0)
            if not isinstance(curr,list):
                rels.add(curr.tup())
            else:
                assert not stack # only the last element should be a list
                stack = flatten(curr)
        return rels
##############################################################################
class PK(Expr):
    '''Special Expr type for providing PK + UID info'''
    def __init__(self,pk:'PathAttr')->None:
        self.pk = pk
    @property
    def name(self) -> str: return 'PK'
    def show(self,f:Fn)->str:
        return f(self.pk)
    def fields(self) -> list:
        return [self.pk]

############################
# Specific Exprs and Funcs #
############################

Zero  = Literal(0)
One   = Literal(1)
true  = Literal('true')
false = Literal('false')

def Sum(iterable : L[Expr])-> Expr:
    '''The builtin 'sum' function doesn't play with non-integers'''
    return reduce(add, iterable, Zero)

def R2(e1 : Expr, e2 : Expr) -> Expr:
    '''
    Pearson correlation coefficient for two independent vars
    "Goodness of fit" for a model y=x, valued between 0 and 1
    '''
    return (AVG(e1*e2)-(AVG(e1)*AVG(e2))) / (STD(e1) * STD(e2))

def toDecimal(e : Expr) -> Expr:
    return CONVERT(e,Decimal())
