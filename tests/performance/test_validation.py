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

import multiprocessing as mp
from functools import partial
from typing import Any, Dict, List, Optional, Set, Tuple, Type

import pytest
from faker import Faker
from pydantic import BaseModel, ValidationError
from pydantic import validator as pyvalidator
from pydantic.error_wrappers import ErrorWrapper
from pydantic.errors import MissingError
from pydantic.fields import Undefined
from pydantic.main import validate_model
from pydantic.validators import bool_validator, float_validator, int_validator, str_validator

from dbgen import Entity

pytestmark = pytest.mark.skip('performance tests')

SetStr = Set[str]
DictStrAny = Dict[str, Any]
ROOT_KEY = '__root__'


class BenchmarkEntity(Entity, table=True, id_all=True):
    int_val: int
    str_val: str
    bool_val: bool
    float_val: float
    list_val: List[int]

    @pyvalidator('str_val')
    def add_test(value):
        return value + '-test'

    @pyvalidator('int_val', 'float_val')
    def add_one(value):
        return value + 1

    @pyvalidator('bool_val')
    def inverse(value):
        return not value

    @pyvalidator('str_val', pre=True)
    def bad_str(value):
        if not isinstance(value, str):
            return '<nostring>'
        return value


faker = Faker()

data = [
    {
        'int_val': faker.random_int(),
        'str_val': faker.random_int(),
        'bool_val': faker.boolean(),
        'float_val': faker.random_int(),
        'list_val': [faker.random_int()],
    }
    for _ in range(100)
]


def basic_validator(values):
    values['int_val'] = int_validator(values['int_val'])
    values['bool_val'] = bool_validator(values['bool_val'])
    values['str_val'] = str_validator(values['str_val'])
    values['float_val'] = float_validator(values['float_val'])
    return values


bad_fields = {'id', 'gen_id', 'created_at'}


def validate_model_version(data):
    values, *_ = validate_model(BenchmarkEntity, data)
    return {k: v for k, v in values.items() if k not in bad_fields}


def builtin_validator(values):
    values['int_val'] = int(values['int_val'])
    values['bool_val'] = bool(values['bool_val'])
    values['str_val'] = str(values['str_val'])
    values['float_val'] = float(values['float_val'])
    return values


def get_validators(input_data):
    values = {}
    for name, field in BenchmarkEntity.__fields__.items():
        value = input_data.get(name, Undefined)
        if value is not Undefined:
            value, errors = field.validate(value, values, loc=name, cls=BenchmarkEntity)
            if errors:
                if not isinstance(errors, list):
                    errors = [errors]
                raise ValidationError(errors, BenchmarkEntity)
            values[name] = value

    return values


def no_validators(input_data):
    return input_data


_missing = object()


def full_get_validators(input_data, cls, use_default: bool = True):
    values = {}
    errors = []
    for name, field in cls.__fields__.items():
        value = input_data.get(field.alias, _missing)
        if value is _missing and field.alt_alias:
            value = input_data.get(field.name, _missing)
        if value is _missing and use_default:
            value = field.get_default()
        if value is not _missing:
            v_, errors_ = field.validate(value, values, loc=field.alias, cls=cls)
            if isinstance(errors_, ErrorWrapper):
                errors.append(errors_)
            elif isinstance(errors_, list):
                errors.extend(errors_)
            else:
                values[name] = v_
    if errors:
        raise ValidationError(errors, BenchmarkEntity)
    return values


def custom_validate_model(  # noqa: C901 (ignore complexity)
    input_data: "DictStrAny", model: Type[BaseModel]
) -> Tuple["DictStrAny", "SetStr", Optional[ValidationError]]:
    """
    validate data against a model.
    """
    values = {}
    errors = []
    # field names, never aliases
    fields_set = set()
    config = model.__config__

    for validator in model.__pre_root_validators__:
        try:
            input_data = validator(model, input_data)
        except (ValueError, TypeError, AssertionError) as exc:
            return {}, set(), ValidationError([ErrorWrapper(exc, loc=ROOT_KEY)], model)

    for name, field in model.__fields__.items():
        value = input_data.get(field.alias, _missing)
        if value is _missing and config.allow_population_by_field_name and field.alt_alias:
            value = input_data.get(field.name, _missing)

        if value is _missing:
            if field.required:
                errors.append(ErrorWrapper(MissingError(), loc=field.alias))
                continue

            value = field.get_default()

            if not config.validate_all and not field.validate_always:
                values[name] = value
                continue
        else:
            fields_set.add(name)

        v_, errors_ = field.validate(value, values, loc=field.alias, cls=model)
        if isinstance(errors_, ErrorWrapper):
            errors.append(errors_)
        elif isinstance(errors_, list):
            errors.extend(errors_)
        else:
            values[name] = v_

    for skip_on_failure, validator in model.__post_root_validators__:
        if skip_on_failure and errors:
            continue
        try:
            values = validator(model, values)
        except (ValueError, TypeError, AssertionError) as exc:
            errors.append(ErrorWrapper(exc, loc=ROOT_KEY))

    if errors:
        return values, fields_set, ValidationError(errors, model)
    else:
        return values, fields_set, None


validators = [
    lambda x: x,
    basic_validator,
    builtin_validator,
    validate_model_version,
    get_validators,
    partial(full_get_validators, cls=BenchmarkEntity),
    partial(custom_validate_model, model=BenchmarkEntity),
]
ids = [
    'no_validator',
    'basic_validators',
    'builtin_validator',
    'validate_model_version',
    'get_validators',
    'full_get_validators',
    'custom_validate_model',
]


@pytest.mark.parametrize('validator', validators, ids=ids)
def test_simple_validation(validator, benchmark):
    benchmark(lambda: list(map(validator, data)))


@pytest.fixture
def mp_pool():
    pool = mp.Pool(mp.cpu_count() - 1)
    yield pool
    pool.close()


@pytest.mark.parametrize(
    'validator',
    [
        get_validators,
        validate_model_version,
        partial(full_get_validators, cls=BenchmarkEntity),
        no_validators,
        partial(custom_validate_model, model=BenchmarkEntity),
    ],
    ids=['get_validators', 'validate_model_version', 'full_model', 'no_validator', 'custom_validator'],
)
def test_simple_validation_parallel(validator, benchmark, mp_pool):
    benchmark(lambda: list(mp_pool.map(validator, data)))


@pytest.mark.parametrize(
    'validator', [get_validators, validate_model_version, partial(full_get_validators, cls=BenchmarkEntity)]
)
def test_validator_only(validator):
    val = {'int_val': 0.1, 'str_val': 0.1, 'bool_val': False, 'float_val': 0.1, 'list_val': [0]}
    output = validator(val)

    assert output['int_val'] == 1
    assert output['str_val'] == '<nostring>-test'
    assert output['float_val'] == 1.1
    assert output['bool_val']
