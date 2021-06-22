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
from os.path import dirname, join

from dbgen import Env, Import

ROOT = join(dirname(__file__), 'data')

defaultEnv = Env(
    [
        Import("json", ["load"]),
        Import("typing", aliased_imports={"List": "L", "Tuple": "T"}),
        Import("dbgen.example.constants", "ROOT"),
        Import("os.path", "join"),
    ]
)