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

from typer import Argument, Option

from dbgen.cli.utils import chdir_callback, config_callback, set_verbosity, version_callback

model_string_option = Option(None, '--model', envvar=["DBGEN_MODEL_STR", "dbgen_model_str"])
model_arg_option = Argument('--model', envvar=["DBGEN_MODEL_STR", "dbgen_model_str"])
version_option = Option(None, "--version", callback=version_callback, is_eager=True)
config_option = Option(
    '.env', "--config", "-c", callback=config_callback, help="Configuration file.", envvar='DBGEN_CONFIG'
)
log_file_option = Option(None, "--log-file", help="File to write logs to")
verbose_option = lambda v=True: Option(
    v, "--verbose", "-v", callback=set_verbosity, help="Increases the verbosity of printed messages"
)
chdir_option = Option(
    None,
    "--chdir",
    callback=chdir_callback,
    is_eager=True,
    help="Change the working directory to find the model",
)
