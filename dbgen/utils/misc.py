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
    '''
    Common methods shared by many DbGen objects
    '''
    @abstractmethod
    def __str__(self)->str:
        raise NotImplementedError

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other : Any) -> bool:
        '''
        Maybe the below should be preferred? Try it out, sometime!
        return type(self) == type(other) and vars(self) == vars(other)

        '''
        if type(other) == type(self):
            return vars(self) == vars(other)
        else:
            args = [self,type(self),other,type(other)]
            raise ValueError('Equality type error \n{} \n({}) \n\n{} \n({})'.format(*args))

    def copy(self : T) -> T:
        return deepcopy(self)

    def toJSON(self) -> str:
        for v in vars(self).values():
            if ' at 0x' in str(v):  # HACK
                raise ValueError('serializing an object with reference to memory:'+ str(vars(self)))
        return encode(self,make_refs=False)

    @staticmethod
    def fromJSON(s : str) -> 'Base':
        return decode(s)

    def __hash__(self)->int:
        return hash(self.toJSON())

    @property
    def hash(self) -> int:
        return hash_(self.toJSON())
