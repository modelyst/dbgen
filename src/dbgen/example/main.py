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

# External
from os import environ
from os.path import join

# Internal
from dbgen import ConnectInfo as Conn
from dbgen import Model
from dbgen.example.generators.analysis import analysis
from dbgen.example.generators.io import io
from dbgen.example.schema import all
from dbgen.utils.parsing import parser

################################################################################

root = join(environ["HOME"], "Documents/JSON/")
db = Conn(user="michaeljstatt", db="test", schema="example")
mdb = db.copy()
mdb.schema = db.schema + "_log"


def make_model() -> Model:
    # Initialize model
    m = Model("example")

    # Add objects and relations
    m.add(all)  # type: ignore

    # Add Generators
    io(m)
    analysis(m)
    return m


model = make_model()


def main(args: dict) -> None:

    # Run model
    args["nuke"] = "T"
    model.run(conn=db, meta_conn=mdb, **args)


if __name__ == "__main__":
    args = parser.parse_args()
    main(vars(args))
