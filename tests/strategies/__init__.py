#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import inspect
from string import ascii_lowercase
from typing import Callable

from hypothesis import assume
from hypothesis import strategies as st
from hypothesis.strategies._internal.strategies import SearchStrategy

from dbgen.core.args import Arg, Const
from dbgen.core.func import Env, Func, Import
from dbgen.core.load import Load, LoadEntity
from dbgen.core.transforms import PyBlock
from dbgen.utils.misc import reserved_words
from tests.example_functions import example_callables

no_colons = st.text(
    st.characters(whitelist_categories=("Lu", "Ll"), blacklist_characters=(":")),
    min_size=1,
)
lowercase = lambda x=1, y=10: st.text(min_size=x, max_size=y, alphabet=ascii_lowercase)


@st.composite
def no_python_reserved_words(draw: Callable, min_size: int = 1, max_size: int = 10):
    out = draw(lowercase(min_size, max_size))
    assume(out not in reserved_words)
    return out


libs = st.sampled_from(["math", "datetime", "uuid"])
packages_strat = st.sampled_from(
    [
        ("math", ("cos", "tan", "sin")),
        ("datetime", ("date", "datetime")),
        ("uuid", ("UUID", "uuid4")),
    ]
)
aliased_lib_strat = st.builds(
    Import,
    lib=libs,
    lib_alias=no_python_reserved_words(),
    aliased_imports=st.just(set()),
    unaliased_imports=st.just(dict()),
)


@st.composite
def with_imports_strat(draw, module=None, packages=None):
    if not (module and packages):
        module, packages = draw(packages_strat)
    return draw(
        st.builds(
            Import,
            lib=st.just(module),
            lib_alias=st.just(None),
            unaliased_imports=st.sets(st.sampled_from(packages), min_size=1, max_size=3),
            aliased_imports=st.dictionaries(
                keys=st.sampled_from(packages),
                values=no_python_reserved_words(),
                min_size=1,
                max_size=3,
            ),
        )
    )


import_strat = st.one_of(aliased_lib_strat, with_imports_strat())
env_strat = st.builds(Env, imports=st.lists(import_strat, max_size=3))


def basic_function(arg_1: int, arg_2: str, arg_3: float) -> str:
    return f"{arg_1}->{arg_2}->{arg_3}"


basic_lambda = lambda arg_1, arg_2, arg_3: f"{arg_1}->{arg_2}->{arg_3}"

function_strat = st.one_of(list(map(st.just, example_callables)))  # type: ignore
func_strat = st.one_of(list(map(st.just, map(Func.from_callable, example_callables))))
any_strat = st.one_of(st.floats(), st.just(None), st.text(), st.booleans(), st.integers())
const_strat = st.builds(
    Const,
    val=any_strat,
)
arg_strat = st.builds(
    Arg,
    key=lowercase(),
    name=lowercase(),
)
arg_like_strat = st.one_of(arg_strat, const_strat)
primary_key_strat = st.one_of(arg_strat, st.builds(Const, val=st.just(None)))


@st.composite
def get_pyblock_strat(draw: Callable, function: Callable = None) -> st.SearchStrategy[PyBlock]:
    if not function:
        function = draw(function_strat)
    sig = inspect.signature(function)
    print(function)
    n_args = len(sig.parameters)
    return draw(
        st.builds(
            PyBlock,
            env=env_strat,
            function=st.just(function),
            inputs=st.lists(
                st.one_of(st.builds(Arg), st.builds(Const, val=st.floats())),
                min_size=n_args,
                max_size=n_args,
            ),
            outnames=st.lists(st.text(), min_size=1, unique=True),
        )
    )


pyblock_strat = get_pyblock_strat()
datatypes_strat = st.sampled_from(("str", "int", "float"))
# Load Entities;
load_entity_strat_1 = lambda id_fks: st.builds(
    LoadEntity,
    entity_name=lowercase(),
    primary_key_name=lowercase(),
    identifying_attributes=st.dictionaries(lowercase(), datatypes_strat, min_size=1),
    identifying_foreign_keys=st.sets(lowercase(), max_size=id_fks),
)
load_entity_strat_2 = lambda id_fks: st.builds(
    LoadEntity,
    entity_name=lowercase(),
    primary_key_name=lowercase(),
    identifying_attributes=st.dictionaries(lowercase(), datatypes_strat),
    identifying_foreign_keys=st.sets(lowercase(), min_size=min(id_fks, 1), max_size=id_fks),
)
load_entity_strat = lambda id_fks: st.one_of(load_entity_strat_1(id_fks), load_entity_strat_2(id_fks))

# Loads

attrs_strat = st.dictionaries(keys=lowercase(), values=arg_like_strat)
basic_update_load_strat = st.builds(
    Load,
    primary_key=arg_strat,
    insert=st.just(False),
    load_entity=load_entity_strat(0),
    inputs=attrs_strat,
)


@st.composite
def get_basic_insert_load_strat(draw, fks=st.just({}), primary_key=st.just(None)) -> SearchStrategy[Load]:
    load_entity = draw(load_entity_strat(0))
    attrs = {}
    for id_attr in load_entity.identifying_attributes:
        attrs[id_attr] = draw(arg_strat)
    attrs.update(draw(attrs_strat))
    return draw(
        st.builds(
            Load,
            primary_key=primary_key,
            insert=st.just(True),
            inputs=st.just(attrs),
            load_entity=st.just(load_entity),
        )
    )


basic_insert_load_strat = get_basic_insert_load_strat()
basic_load_strat = st.one_of(basic_insert_load_strat, basic_update_load_strat)
basic_attrs = st.dictionaries(
    keys=lowercase(),
    values=arg_like_strat,
)
foreign_key_strat = lambda fk_names: st.dictionaries(keys=fk_names, values=basic_load_strat, min_size=1)


@st.composite
def recursive_load_strat(draw: Callable) -> SearchStrategy[Load]:
    load_entity = draw(load_entity_strat(1))
    attrs = draw(basic_attrs)
    fks = draw(foreign_key_strat(lowercase()))
    fks = {key: val["out"] for key, val in fks.items()}
    for id_attr in load_entity.identifying_attributes:
        attrs[id_attr] = draw(arg_strat)

    for id_fk in load_entity.identifying_foreign_keys:
        fks[id_fk] = draw(basic_load_strat)["out"]
    out = draw(
        st.builds(
            Load,
            inputs=st.just({**attrs, **fks}),
            load_entity=st.just(load_entity),
        )
    )
    return out
