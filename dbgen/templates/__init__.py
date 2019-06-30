from jinja2 import Environment as JinjaEnv, PackageLoader
jinja_env    = JinjaEnv(loader = PackageLoader('dbgen','templates'))
