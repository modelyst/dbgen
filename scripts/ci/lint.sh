#!/usr/bin/env bash

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

set -e
set -x
# Get pydasher version
python -c 'import pydasher; print(pydasher.__version__)'
flake8 --version
flake8 src/dbgen tests docs_src
black --version
black src/dbgen tests docs_src --check
isort --version
isort src/dbgen tests docs_src --check-only
mypy --version
mypy --config setup.cfg src/dbgen
