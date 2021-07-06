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
import readline
import time
from functools import reduce
from pathlib import Path
from typing import Any
from typing import Callable as C
from typing import Dict as D
from typing import List as L
from typing import Optional
from typing import Set as S
from typing import Tuple as T

import typer
from click import Choice

import dbgen.cli.styles as styles
import dbgen.utils.settings as settings
from dbgen.cli.utils import config_option, validate_model_str
from dbgen.core.gen import Generator
from dbgen.utils.config import config
from dbgen.utils.sql import Connection as Conn
from dbgen.utils.sql import DictCursor

BAD_GENERATOR = "generator invalid, please a generator from models gens:\n {0}"
MISSING_GENERATOR = "Missing argument 'GENERATOR'. Please select from the model's gens:\n\t- {0}"


def test(
    generator: str = typer.Option(None, "--generator", help="Model string in package:function/model format."),
    model_str: str = typer.Option(None, "--model", help="Model string in package:function/model format."),
    limit: int = 5,
    interact: bool = False,
    config_file: Optional[Path] = config_option,
):
    """Interactively test a generator through CLI"""
    # Update the config with the cmd line config
    if config_file:
        config.read(config_file)
    # Initialize settings to get the connections
    settings.initialize()
    # Validate model_str
    model_str = model_str or config.get("core", "model_str")
    model = validate_model_str(model_str)
    # Validate Generator
    if generator is None:
        raise typer.BadParameter(MISSING_GENERATOR.format("\n\t- ".join(sorted(model.gens.keys()))))
    try:
        model._validate_name(generator)
    except ValueError:
        raise typer.BadParameter(BAD_GENERATOR.format("\n".join(model.gens.keys())))
    if generator not in model.gens:
        raise typer.BadParameter(BAD_GENERATOR.format("\n".join(model.gens.keys())))
    # Validate Generator
    typer.echo(f"Testing Generator {styles.greens(generator)} in model {styles.reds(model.name)}")
    assert settings.CONN
    test_with_db(
        model.gens[generator],
        universe=model._get_universe(),
        db=settings.CONN,
        interact=interact,
        limit=limit,
    )


def test_with_db(
    generator: Generator,
    universe,
    db: Conn = None,
    limit: int = 5,
    rename_dict: bool = True,
    interact: bool = False,
    input_rows: L[dict] = [],
) -> T[L[D[str, dict]], L[D[str, L[dict]]]]:
    assert (
        limit <= 200 or not interact
    ), "Don't allow for more than 200 rows with test with db when interact is used."
    assert (
        db is not None or input_rows
    ) or generator.query is None, "Need to provide a db connection if generator has a query"

    if db is not None and generator.query is not None:
        cursor = db.connect(auto_commit=False).cursor(f"test-{generator.name}", cursor_factory=DictCursor)
        # If there is a query get the row count and execute it
        with typer.progressbar(length=3, label="Executing Querying...") as progress:
            query_str = generator.query.showQ(limit=limit)
            time.sleep(0.3)
            cursor.execute(query_str)
            progress.update(1)
            time.sleep(0.3)
            progress.label = "Fetching Rows..."
            input_rows.extend(cursor.fetchall())
            progress.update(1)
            time.sleep(0.3)
            progress.label = "Closing Connection"
            cursor.close()
            progress.update(1)

    if interact:
        return interact_gen(universe, generator, input_rows)
    else:
        if generator.query is None and len(input_rows) == 0:
            input_rows = [{}]
        typer.echo(f"Testing {len(input_rows)} rows...")
        out = generator.test(universe, input_rows, rename_dict)
        styles.good_typer_print("Finished Testing!")
        return out


def interact_gen(
    objs, gen: "Generator", input_rows: L[dict], max_rows: int = 200
) -> T[L[D[str, dict]], L[D[str, L[dict]]]]:
    """
    Allows a CLI for interacting with a generator given a set of input rows

    Args:
        gen (Gen): generator to interact with
        input_rows (L[dict]): list of input rows that simulate the query input

    Raises:
        ImportError: if prettytable, pprint and readline not installed (didn't want to require these in the requirements.txt just for this function)

    Returns:
        L[dict]: output of the generator
    """
    # Attempt to import necessary functions for formmatting
    try:
        from pprint import pprint

        from prettytable import PrettyTable  # type: ignore
    except ImportError:
        raise ImportError("Need prettytable for interact mode")

    # Initialize output list and valid responses for the outer question
    post_pyblocks: L[dict] = []
    load_dicts: L[dict] = []
    # Initialize the response to user input and formatting styles.delimiter func
    answer = ""
    # Pretty table for displaying input row data
    if input_rows:
        x = PrettyTable(field_names=list(input_rows[0].keys()))
    else:
        styles.bad_typer_print("Generator has no inputs!")

    while len(input_rows) > 0 and answer != "q" and answer != "Q":
        print_len = 200
        next_row = dict(input_rows.pop(0))
        next_row_str = lambda x: (str(next_row)[:x] + "..." if len(str(next_row)) > x else dict(next_row))
        typer.clear()
        typer.echo("Next Row:")
        x.add_row(next_row.values())
        pprint(next_row_str(print_len))
        answer = ""
        while answer.lower() not in (
            "n",
            "s",
        ):
            if answer == "m":
                print_len += 200
                pprint(next_row_str(print_len))
            else:
                styles.delimiter()

            answer = typer.prompt(
                "Next (n), quit (q), skip (s), or expand current row (m)?",
                type=Choice(['n', 'q', 'm', 's'], case_sensitive=False),
                default=answer or "n",
            ).lower()

            if answer.lower() == "q":
                typer.secho("Qutting...", fg=typer.colors.BRIGHT_MAGENTA)
                raise typer.Exit()
        typer.clear()
        # Skip the row and clear the pretty table
        if answer == "s":
            x.clear_rows()
            continue
        styles.delimiter()
        styles.good_typer_print("Processing Row...")
        list_of_processed_namespaces, curr_load_dicts = gen.test(objs, [next_row], verbose=True)
        curr_output = list_of_processed_namespaces[0]
        curr_load_dict = curr_load_dicts[0]
        typer.clear()

        completer = get_completer(list(curr_output.keys()))
        readline.parse_and_bind("tab: complete")
        readline.set_completer(completer)
        display = ""
        styles.delimiter()
        i = 0
        while display != "q" and display != "Q":
            if "query" in curr_output:
                typer.echo("Query:")
                typer.echo("\t- " + "query")
            typer.echo("PyBlock Names:")
            for key in curr_output.keys():
                if key != "query":
                    typer.echo("\t- " + key)
            styles.delimiter()
            display = typer.prompt(
                "What pyblock to display? (tab to see options/q to quit/n to move to loads)",
                default=list(curr_output.keys())[i % len(curr_output)],
                type=Choice(list(curr_output.keys()) + ['q', 'Q', 'n', 'N']),
                show_choices=False,
            )
            i += 1
            if display in curr_output:
                styles.delimiter()
                if display == "query":
                    typer.echo("Query Row:")
                else:
                    typer.echo(f"Function Name: {display}")
                typer.echo("Output:")
                try:
                    pprint(curr_output[display])
                except KeyboardInterrupt:
                    pass
                styles.delimiter()
                while True:
                    if (
                        typer.prompt(
                            "Press enter to continue", default="enter", show_default=False, prompt_suffix=""
                        )
                        == "enter"
                    ):
                        typer.clear()
                        break
            elif display.lower() == "q":
                break
            elif display.lower() == "n":
                break
            else:
                typer.clear()
                styles.delimiter()
                typer.echo(f"Key not found in processed dict: {display}")
                styles.delimiter()

        completer = get_completer(list(curr_load_dict.keys()))
        readline.set_completer(completer)
        valid_keys = list(curr_load_dict.keys())
        valid_key_choice = Choice(valid_keys + ['q', 'Q', 'n', 'N'], case_sensitive=True)
        i = 0

        def print_load_header():
            typer.echo("Load Names:")
            for key in curr_load_dict.keys():
                typer.echo("\t- " + key + f" ({len(curr_load_dict[key])} rows)")
            styles.delimiter()

        typer.clear()
        print_load_header()
        while display.lower() != "q":
            display = typer.prompt(
                "What load to display? (tab to see options/q to Quit/n to move to next row)",
                type=valid_key_choice,
                show_choices=False,
                default=valid_keys[i % len(valid_keys)],
            ).lower()
            i += 1
            if display in curr_load_dict:
                typer.clear()
                typer.echo()
                styles.delimiter()
                typer.echo(f"Table Name: {display}")
                typer.echo("Output:")
                try:
                    rows = curr_load_dict[display]
                    if rows:
                        all_keys: S[str] = reduce(
                            lambda prev, next: prev.union(set(next.keys())),
                            rows,
                            set(),
                        )
                        table = PrettyTable(field_names=list(all_keys))
                        for row in curr_load_dict[display][:max_rows]:
                            row_data = list(row.get(key) for key in all_keys)
                            row_data = [(data if data != "" else '""') for data in row_data]
                            table.add_row(row_data)
                        typer.echo(table)
                    else:
                        typer.echo("No rows to be updated or inserted")
                except KeyboardInterrupt:
                    pass
                styles.delimiter()
                while True:
                    if (
                        typer.prompt(
                            "Press enter to continue", default="enter", prompt_suffix="", show_default=False
                        )
                        == "enter"
                    ):
                        typer.clear()
                        print_load_header()
                        break
            elif display == "":
                typer.clear()
                print_load_header()
                continue
            elif display.lower() == "q":
                typer.secho("Qutting...", fg=typer.colors.BRIGHT_MAGENTA)
                raise typer.Exit()
            elif display.lower() == "n":
                break
            else:
                styles.delimiter()
                typer.echo(f"Key not found in Load Dict: {display}")
                styles.delimiter()

        post_pyblocks.append(curr_output)
        load_dicts.append(curr_load_dict)
        x.clear_rows()

    typer.echo("No more rows left")
    return post_pyblocks, load_dicts


def get_completer(cmds: L[Any]) -> C[[str, int], Any]:
    def completer(text, state):
        options = [i for i in cmds if i.startswith(text)]
        if state < len(options):
            return options[state]
        else:
            return None

    return completer
