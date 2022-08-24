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
python -c 'import setup; setup.write_version()'
SCRIPT_DIR=$(dirname "$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )")
DBGEN_VERSION=$(python -c "from dbgen.version import version; print(version)")
ENVIRONMENT=${1:-development}
echo "Using Environment $ENVIRONMENT"
echo $SCRIPT_DIR
poetry build
for PYTHON_VERSION in  '3.7' '3.8' '3.9'
do
echo $PYTHON_VERSION-$EXTRAS
docker build . -t dbgen:$VERSION-py$PYTHON_VERSION-pip \
    -f ./docker/dbgen/Dockerfile.pip \
    --build-arg PYTHON_VERSION=$PYTHON_VERSION \
    --build-arg DBGEN_VERSION=$VERSION \
    --build-arg ENVIRONMENT=$ENVIRONMENT
docker run --rm dbgen:$DBGEN_VERSION-py$PYTHON_VERSION-pip python -m pytest -q
done
exit 0
