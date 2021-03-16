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

from json import dumps

from jinja2 import Environment as JinjaEnv
from jinja2 import PackageLoader

jinja_env = JinjaEnv(loader=PackageLoader("dbgen", "templates"))


def escape(input) -> str:
    return dumps(str(input))


jinja_env.filters["escape"] = escape
