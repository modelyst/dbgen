from typing     import Any, Dict, Type, TypeVar, Union as U, List as L
from abc        import ABCMeta, abstractmethod
from copy       import deepcopy
from string import ascii_lowercase
from importlib import import_module
from inspect import getfullargspec
from datetime import date
from hypothesis import infer # type: ignore
from hypothesis.strategies import (SearchStrategy, one_of, booleans, dates, # type: ignore
                                   integers, just, text, builds, none, floats,
                                   dictionaries, lists, recursive)

from json import loads, dumps
# from jsonpickle import encode, decode, tags # type: ignore

from dbgen.utils.str_utils import hash_

##############################################################################
T = TypeVar('T')

def identity(x : T) -> T:
    return x

def kwargs(x: Any) -> L[str]:
    return sorted(getfullargspec(type(x))[0][1:])

anystrat = one_of(text(), booleans(), text(), dates(), integers(), none())
nonempty = text(min_size=1)
letters  = text(min_size=1,alphabet=ascii_lowercase)
jsonstrat = recursive(none() | booleans() | floats() | text(),
                      lambda children: lists(children, 1) |
                      dictionaries(text(), children, min_size=1))

def build(typ:Type) -> SearchStrategy:
    """Unfortunately, hypothesis cannot automatically ignore default kwargs."""
    args,_,_,_,_,_,annotations = getfullargspec(typ)
    # use default kwarg value if type is Any
    kwargs = {k:infer for k in args[1:] if annotations[k]!=Any}
    return builds(typ, **kwargs)

simple = ['int','str','float','NoneType','bool','date']
complex = ['tuple','list','set','dict']

def to_dict(x: Any) -> Dict[str, Any]:
    '''Create JSON serializable structure for arbitrary Python/DbGen type.'''
    ptype = type(x).__name__
    metadata = dict(_module=type(x).__module__,
                    _pytype=ptype) # type: Dict[str, Any]
    if metadata['_module'] in ['builtins', 'datetime']:
        if ptype == 'date':
            assert metadata['_module'] == 'datetime'
            value = x.isoformat()
        elif ptype in simple:
            value = x
        elif ptype in complex:
            assert hasattr(x,'__iter__')
            value = [(to_dict(k), to_dict(v)) for k,v in x.items()] \
                    if ptype == 'dict' else [to_dict(xx) for xx in x] # type: ignore
        else:
            raise TypeError(x)
        return dict(**metadata, _value=value)
    else:
        assert hasattr(x,'__dict__'), metadata
        data = {k:to_dict(v) for k,v in vars(x).items() if
                (k in kwargs(x)) or k[0]!='_'}
        #if ' at 0x' in str(v):  raise ValueError('serializing an object with reference to memory:'+ str(vars(self)))
        metadata['_uid'] = hash_([data[k] for k in kwargs(x)])
        return {**metadata,**data}

def from_dict(dic:Dict[str, Any]) -> Any:
    '''Create a python/DbGen type from a JSON serializable structure.'''
    assert isinstance(dic,dict)
    mod, ptype = dic.pop('_module'), dic.pop('_pytype')
    if (mod, ptype) == ('builtins', 'NoneType'):
        return None
    else:
        constructor = getattr(import_module(mod), ptype)
        if '_value' in dic:
            val = dic['_value']
            if ptype == 'date':
                return date.fromisoformat(val)
            elif ptype in simple:
                return constructor(val)
            elif ptype == 'dict':
                return {from_dict(k):from_dict(v) for k,v in val}
            else:
                return constructor([from_dict(x) for x in val])
        else:
            return constructor(**{k:from_dict(v) for k,v in dic.items()
                                 if k in getfullargspec(constructor)[0][1:]})


class Base(object,metaclass=ABCMeta):
    '''Common methods shared by many DbGen objects.'''

    def __init__(self) -> None:
        fields = set(vars(self))
        args = set(kwargs(self))
        missing = args - fields
        assert not missing, 'Need to store args {} of {}'.format(missing, self)

    @abstractmethod
    def __str__(self)->str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def strat(cls) -> SearchStrategy:
        """A hypothesis strategy for generating random examples."""
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
            args = [self, type(self), other, type(other)]
            err = 'Equality type error \n{} \n({}) \n\n{} \n({})'
            raise ValueError(err.format(*args))

    def copy(self : T) -> T:
        return deepcopy(self)

    def toJSON(self) -> str:
        return dumps(to_dict(self))
        # try: return dumps(to_dict(self))
        # except Exception as e: print(e); import pdb;pdb.set_trace(); assert False
    @staticmethod
    def fromJSON(s : str) -> 'Base':
        val = from_dict(loads(s))
        if not isinstance(val,Base):
            import pdb;pdb.set_trace()
        assert isinstance(val,Base)
        return val

    def __hash__(self)->int:
        return self.hash

    @property
    def hash(self) -> int:
        return to_dict(self)['_uid']



# ######################
# # Validate Json for security
# # --------------------
#
#
# # JSONPICKLE Keys
# # --------------------
# OBJECT = 'py/object'
# SET    = 'py/set'
# TUPLE  = 'py/tuple'
# TYPE   = 'py/type'
# valid_jsonpickle_keys = [OBJECT, SET, TUPLE, TYPE]
# valid_objects  = ['dbgen.core', 'networkx.classes.digraph.DiGraph']
# valid_builtins = ['builtins.dict']
# def validate_dict(dict_entry : dict)->bool:
#     assert any(jsonpickle_key in dict_entry for jsonpickle_key in valid_jsonpickle_keys), \
#     f'jsonpickle_key not valid:\njson_pickle_key: {dict_entry.keys()}\ndict entry: {dict_entry}'
#     if OBJECT in dict_entry:
#         object_type = dict_entry['py/object']
#         assert any(obj in object_type for obj in valid_objects),\
#         f'object type type not valid:\object type: {object_type}\ndict entry: {dict_entry}'
#     elif TYPE in dict_entry:
#         builtin_type = dict_entry['py/type']
#         assert any(builtins in builtin_type for builtins in valid_builtins), \
#         f'Built in type not valid:\nbuilt in type: {builtin_type}\ndict entry: {dict_entry}'
#     return True
#
#
# json_types = U[str, int, float, bool, list, tuple, dict]
# def validate_json_obj(json_obj : json_types)->bool:
#     if isinstance(json_obj,type(None)):
#         return True
#     elif isinstance(json_obj, dict):
#         # Check for jsonpickle keys that will cause python to be executed
#         # Need to ensure it only executes functions we trust
#         if any(key in json_obj for key in tags.RESERVED):
#             validate_dict(json_obj)
#         for key, val in json_obj.items():
#             if isinstance(val,(dict, list)):
#                  validate_json_obj(val)
#     elif isinstance(json_obj,(list,tuple)):
#         for val in json_obj:
#             validate_json_obj(val)
#     elif isinstance(json_obj,str):
#         pass
#     else:
#         import pdb; pdb.set_trace()
#         raise ValueError(f'{json_obj} failed validation')
#     return True
