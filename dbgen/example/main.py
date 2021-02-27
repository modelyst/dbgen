# External
from os import environ
from os.path import join

# Internal
from dbgen import Model, ConnectInfo as Conn
from dbgen.example.schema import all
from dbgen.example.generators.io import io
from dbgen.example.generators.analysis import analysis
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
