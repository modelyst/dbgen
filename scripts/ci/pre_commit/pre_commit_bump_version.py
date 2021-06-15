#!python
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

"""
Module to check bump version
"""
import re
import subprocess
import sys

import toml

# def main() -> int:
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--max-length', help="Max length for hook names")
#     args = parser.parse_args())
#     max_length = int(args.max_length) or 70

#     retval = 0

#     with open('.pre-commit-config.yaml', 'rb') as f:
#         content = yaml.safe_load(f)
#         errors = get_errors(content, max_length)
#     if errors:
#         retval = 1
#         print(f"found pre-commit hook names with length exceeding {max_length} characters")
#         print("move add details in description if necessary")
#     for hook_name in errors:
#         print(f"    * '{hook_name}': length {len(hook_name)}")
#     return retval


def get_current_version():
    # Check if version has already been bumped
    out = subprocess.check_output(
        [
            "git",
            "diff",
            "--staged",
            "./src/dbgen/version.py",
        ]
    ).decode()
    # Looks for addition of line in format "version = XX.YY.ZZ"
    match = re.search(r"\+version = \"(\d+\.\d+\.\d+)\"", out)
    if match:
        curr_version = match.groups()[0]
    else:
        # If no version bumping detected
        ret_val, error = 0, None
        version_pattern = r"^version\s*=\s*\"(.*)\""
        version_format = r"(\d+)\.(\d+)\.(\d+)"
        with open("./src/dbgen/version.py") as f:
            contents = f.read()
            out = re.search(version_pattern, contents, re.MULTILINE)
        if out is None:
            ret_val = 1
            error = "Can not parse version from ./src/dbgen/version.py"
            return ret_val, error, None
        else:
            assert len(out.groups()) == 1
            curr_version = out.groups()[0]
            match = re.search(version_format, curr_version)
            if not match:
                ret_val = 1
                error = f"Current version in bad format!\nCurrent Version: {curr_version}"
                return ret_val, error, None
            major, minor, patch = match.groups()
            new_version = f"{major}.{minor}.{int(patch)+1}"
            print(
                f"There are staged python files without a bump to the version.\nYou need to bump version at least from {curr_version} to {new_version}!"
            )
            return 1
    poetry_version = toml.load("pyproject.toml")["tool"]["poetry"]["version"]
    assert (
        curr_version == poetry_version
    ), f"Pyproject.toml out of sync with dbgen version.\nDBgen version: {curr_version}\npyproject.toml: {poetry_version}"
    return 0


if __name__ == '__main__':
    sys.exit(get_current_version())
