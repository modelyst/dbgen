# External Modules
from os.path import join

# Internal Modules
from dbgen import parser, ConnectInfo
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
