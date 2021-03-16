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

# External Modules
from os.path import join

# Internal Modules
from dbgen import ConnectInfo, parser

from .schema import make_model  # type: ignore
from .utils import get_config

"""
Run a model
"""
################################################################################


def main(args: dict) -> None:
    """
    Run the model with no extensions from command line.
    """
    config = get_config()
    model_root = config["dbgen"]["model_root"]
    model_name = config["dbgen"]["model_name"]
    m = make_model(model_name)
    db = ConnectInfo.from_file(join(model_root, "dbgen_files/dev.json"))
    mdb = ConnectInfo.from_file(join(model_root, "dbgen_files/log.json"))

    m.run(db, mdb, **args)


if __name__ == "__main__":
    args = parser.parse_args()
    main(vars(args))
