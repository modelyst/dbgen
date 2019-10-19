from typing     import Any, TypeVar, Union as U, List as L
from abc        import ABCMeta, abstractmethod
from copy       import deepcopy

from json import loads
from jsonpickle import encode, decode, tags # type: ignore

from dbgen.utils.str_utils import hash_

##############################################################################
T = TypeVar('T')

def identity(x : T) -> T:
    return x

# ######################
# # Validate Json for security
# # --------------------


# # JSONPICKLE Keys
# # --------------------
OBJECT = 'py/object'
SET    = 'py/set'
TUPLE  = 'py/tuple'
TYPE   = 'py/type'
valid_jsonpickle_keys = [OBJECT, SET, TUPLE, TYPE]
valid_objects  = ['dbgen.core', 'networkx.classes.digraph.DiGraph']
valid_builtins = ['builtins.dict']
def validate_dict(dict_entry : dict)->bool:
    assert any(jsonpickle_key in dict_entry for jsonpickle_key in valid_jsonpickle_keys), \
    f'jsonpickle_key not valid:\njson_pickle_key: {dict_entry.keys()}\ndict entry: {dict_entry}'
    if OBJECT in dict_entry:
        object_type = dict_entry['py/object']
        assert any(obj in object_type for obj in valid_objects),\
        f'object type type not valid:\object type: {object_type}\ndict entry: {dict_entry}'
    elif TYPE in dict_entry:
        builtin_type = dict_entry['py/type']
        assert any(builtins in builtin_type for builtins in valid_builtins), \
        f'Built in type not valid:\nbuilt in type: {builtin_type}\ndict entry: {dict_entry}'
    return True


json_types = U[str, int, float, bool, list, tuple, dict]
def validate_json_obj(json_obj : json_types)->bool:
    if isinstance(json_obj,type(None)):
        return True
    elif isinstance(json_obj, dict):
        # Check for jsonpickle keys that will cause python to be executed
        # Need to ensure it only executes functions we trust
        if any(key in json_obj for key in tags.RESERVED):
            validate_dict(json_obj)
        for key, val in json_obj.items():
            if isinstance(val,(dict, list)):
                 validate_json_obj(val)
    elif isinstance(json_obj,(list,tuple)):
        for val in json_obj:
            validate_json_obj(val)
    elif isinstance(json_obj,str):
        pass
    else:
        import pdb; pdb.set_trace()
        raise ValueError(f'{json_obj} failed validation')
    return True


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

    def toJSON(self,max_depth :int = -2) -> str:
        for v in vars(self).values():
            if ' at 0x' in str(v):  # HACK
                raise ValueError('serializing an object with reference to memory:'+ str(vars(self)))
        return encode(self,make_refs=False, max_depth=max_depth, warn=True)


    @staticmethod
    def fromJSON(s : str) -> 'Base':
        json_obj = loads(s)
        if validate_json_obj(json_obj):
            return decode(s)
        else:
            raise ValueError('failed json validation')

    def __hash__(self)->int:
        return hash(self.toJSON())

    @property
    def hash(self) -> int:
        return hash_(self.toJSON())
