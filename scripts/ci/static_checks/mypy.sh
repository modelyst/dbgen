#!/usr/bin/env bash
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

# shellcheck source=scripts/ci/libraries/_script_init.sh
. "$( dirname "${BASH_SOURCE[0]}" )/../libraries/_script_init.sh"

function run_mypy() {
    local files=()
    if [[ "${#@}" == "0" ]]; then
      files=(dbgen tests docs)
    else
      files=("$@")
    fi

    docker run "${EXTRA_DOCKER_FLAGS[@]}" \
        --entrypoint "/usr/local/bin/dumb-init"  \
        "-v" "${AIRFLOW_SOURCES}/.mypy_cache:/opt/airflow/.mypy_cache" \
        "${AIRFLOW_CI_IMAGE}" \
        "--" "/opt/airflow/scripts/in_container/run_mypy.sh" "${files[@]}"
}

build_images::prepare_ci_build

build_images::rebuild_ci_image_if_needed

run_mypy "$@"
