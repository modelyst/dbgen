from jinja2 import Environment as JinjaEnv, PackageLoader
from json import dumps


jinja_env = JinjaEnv(loader=PackageLoader("dbgen", "templates"))


def escape(input) -> str:
    return dumps(str(input))


jinja_env.filters["escape"] = escape
