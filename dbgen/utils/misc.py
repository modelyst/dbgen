from typing     import Any, TypeVar
from abc        import ABCMeta, abstractmethod
from copy       import deepcopy

from jsonpickle import encode, decode # type: ignore

from dbgen.utils.str_utils import hash_

##############################################################################
T = TypeVar('T')

def identity(x : T) -> T:
    return x # type: ignore


class Base(object,metaclass=ABCMeta):
    @abstractmethod
    def __str__(self)->str:
        raise NotImplementedError

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other : Any) -> bool:
        if type(other) == type(self):
            return vars(self) == vars(other)
        else:
            args = [self,type(self),other,type(other)]
            raise ValueError('Equality type error \n{} \n({}) \n\n{} \n({})'.format(*args))

    def copy(self : T) -> T:
        return deepcopy(self)

    def toJSON(self) -> str:
        return encode(self,make_refs=False)

    @staticmethod
    def fromJSON(s : str) -> 'Base':
        return decode(s)

    def __hash__(self)->int:
        return hash(self.toJSON())

    @property
    def hash(self) -> str:
        return hash_(self.toJSON())
