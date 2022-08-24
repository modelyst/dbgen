#   Copyright 2022 Modelyst LLC
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

from dbgen import Entity, ETLStep, Extract, Model


class MapEntity(Entity, table=True):
    __identifying__ = {
        'label',
    }
    label: str


class CustomExtractor(Extract):
    n: int = 1000

    def extract(self):
        for i in range(self.n):
            yield {'out': i}

    def length(self, **_):
        return self.n


# Set extract
extract = CustomExtractor(n=100)
# Map lambda over arg
map_pb = extract['out'].map(lambda x: str(x))
# insert
map_load = MapEntity.load(insert=True, validation='strict', label=map_pb['out'])
etl_step = ETLStep(name='test_map', transforms=[], loads=[map_load])

model = Model(name='test_map')
model.add_etl_step(etl_step)
