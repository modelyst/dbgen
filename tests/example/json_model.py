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
from uuid import UUID, uuid4
from time import sleep
import requests
from pydantic import HttpUrl
from pydantic.tools import parse_obj_as
from sqlalchemy.sql.expression import text
from sqlmodel import Session, select
import asyncio
from dbgen import Const, Entity, Extract, Generator, Model, Query
from dbgen.configuration import config, get_engines
from dbgen.core.node.transforms import PyBlock


class CustomJsonExtract(Extract):
    url: HttpUrl = parse_obj_as(HttpUrl, 'https://jsonplaceholder.typicode.com/posts')
    outputs: List[str] = ['out', 'uuid']
    _response: dict

    def setup(self, **_):
        self._response = requests.get(self.url).json()
        self._response += [{}]

    def extract(self):
        self._response = requests.get(self.url).json()
        self._response += [{}]
        for row in self._response:
            row['uuid'] = uuid4()
            sleep(0.05)
            yield {'out': row, 'uuid': row['uuid']}

    async def async_extract(self):
        async for row in self.extract():
            yield row
            await asyncio.sleep(0.00005)

    def length(self, **_):
        return 100


class JSONEntityBase(Entity):
    __tablename__ = 'json_entity'
    tags: Optional[List[dict]]
    my_uuid: Optional[UUID]


class JSONEntity(JSONEntityBase, table=True):
    __tablename__ = 'json_entity'
    __identifying__ = {
        'json_val',
    }
    json_val: Optional[dict]


model = Model(name='test_json')
load_json = Generator(name='load_json', loads=[JSONEntity.load(insert=True, json_val=Const({}))])
model.add_gen(load_json)

extract = CustomJsonExtract()
load = JSONEntity.load(insert=True, json_val=extract['out'], my_uuid=extract['uuid'])
load_http_json = Generator(name='load_http_json', extract=extract, loads=[load])
model.add_gen(load_http_json)

query = Query(select(JSONEntity.id, JSONEntity.json_val.op('->')(text("'title'")).label('title')))


def get_title_words(text: str):
    if text:
        return [{'word': word} for word in text.split(' ')]


pb = PyBlock(function=get_title_words, inputs=[query['title']])
load = JSONEntity.load(json_entity=query['id'], tags=pb['out'])
add_tags = Generator(name='add_tags', extract=query, transforms=[pb], loads=[load])
model.add_gen(add_tags)

if __name__ == '__main__':
    main_engine, _ = get_engines(config)
    with Session(main_engine) as session:
        json_entity = session.exec(select(JSONEntity)).first()
        if json_entity:
            print(json_entity.dict().keys())
            print(json_entity.json_val)
