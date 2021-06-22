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

__all__ = ["version"]
from os.path import dirname, exists, join

version = "0.4.6"

try:
    curr_dir = dirname(__file__)
    git_ver_file = join(curr_dir, "git_version")
    if exists(git_ver_file):
        with open(git_ver_file) as f:
            git_version: str = f.read().strip()
    else:
        git_version = ""
except FileNotFoundError:
    git_version = ""
