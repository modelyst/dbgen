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

from typing import List, Optional

from dbgen import Entity, ETLStep, Extract, Model, Query, select, transform


class Person(Entity, table=True):
    __identifying__ = {'name'}
    name: str
    age: Optional[int]


class GetName(Extract[str]):
    names: List[str]
    outputs: List[str] = ['names']

    def extract(self):
        yield from self.names

    def length(self):
        return len(self.names)


with Model(name='test') as model:
    with ETLStep(name='get_names'):
        name = GetName(names=['mike', 'brian', 'kris']).results()

        @transform
        def validate_name(name: str):
            if name == 'kris':
                raise ValueError('bad!')

        validate_name(name)
        Person.load(insert=True, name=name)

    with ETLStep(name='get_age'):
        p_id, name = Query(select(Person.id, Person.name)).results()

        @transform
        def get_age(name: str) -> int:
            return len(name)

        Person.load(id=p_id, age=get_age(name).results())
