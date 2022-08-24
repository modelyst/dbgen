#!/bin/bash
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
# Write Git version
python ./scripts/ci/pre_commit/pre_commit_write_gitversion.py
VERSION=$(python -c "from dbgen import __version__; print(__version__)")
for PYTHON_VERSION in '3.9'
do
echo $PYTHON_VERSION
docker build . -t dbgen:$VERSION-py$PYTHON_VERSION -t dbgen:latest -f ./docker/dbgen/Dockerfile --build-arg PYTHON_VERSION=$PYTHON_VERSION
done
exit 0
