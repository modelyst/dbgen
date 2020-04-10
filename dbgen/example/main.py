# External
from os import environ
from os.path import join

# Internal
from dbgen.core.model.model import Model
from dbgen.core.misc import ConnectInfo as Conn
from dbgen.example.model import all
from dbgen.example.generators.io import io
from dbgen.example.generators.analysis import analysis
from dbgen.utils.parsing import parser

################################################################################
root = join(environ["HOME"], "Documents/JSON/")
db = Conn.from_file(root + "example.json")
mdb = Conn.from_file(root + "example_log.json")


def make_model() -> Model:
    # Initialize model
    m = Model("example")

    # Add objects and relations
    m.add(all)  # type: ignore

    # Add Generators
    io(m)
    analysis(m)
    return m


def main(args: dict) -> None:

    m = make_model()

    # Run model
    args["nuke"] = True
    m.run(conn=db, meta_conn=mdb, **args)


if __name__ == "__main__":
    args = parser.parse_args()
    main(vars(args))
