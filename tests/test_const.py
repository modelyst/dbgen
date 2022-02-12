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

from dbgen import Constant, Entity


class Boring(Entity, table=True):
    __identifying__ = {"dict_attr"}
    dict_attr: dict


list_of_dicts = [{"key": i} for i in range(3)]


def test_list_of_dicts_const():
    Boring.load(insert=True, dict_attr=Constant(list_of_dicts))
