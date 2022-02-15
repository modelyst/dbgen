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

from enum import Enum
from pathlib import Path

import typer

from dbgen.cli.styles import bad_typer_print

try:
    from cookiecutter.main import cookiecutter  # type: ignore

    has_cookiecutter = True
except ImportError:
    has_cookiecutter = False

# Create project from the cookiecutter-pypackage.git repo template
new_app = typer.Typer(name='new')


class Template(str, Enum):
    SIMPLE = 'simple'
    ALICE_BOB_LAB = 'alice-bob-lab'


complexity_map = {
    Template.SIMPLE: ('https://github.com/modelyst/dbgen', 'examples/simple'),
    Template.ALICE_BOB_LAB: ('https://github.com/modelyst/dbgen', 'examples/alice_bob_lab'),
}


@new_app.callback(invoke_without_command=True)
def new(
    template: Template = Template.SIMPLE,
    overwrite_if_exists: bool = typer.Option(False, '--overwrite'),
    output_dir: Path = typer.Option(Path('.'), '--output', '-o'),
    config_file: Path = typer.Option(None, '--config', '-c'),
):

    if not has_cookiecutter:
        from dbgen import __version__

        bad_typer_print(
            f"cookiecutter extra not installed, please install with command:\npython -m pip install 'modelyst-dbgen\\[cookiecutter]'=={__version__}"
        )
        raise typer.Exit(code=2)
    template_url, directory = complexity_map.get(template, template)
    typer.secho(f'Downloading template from {template_url}...', fg='green')
    cookiecutter(
        template_url,
        overwrite_if_exists=overwrite_if_exists,
        output_dir=output_dir,
        config_file=config_file,
        directory=directory,
    )
