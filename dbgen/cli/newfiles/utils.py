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

import configparser

# External imports
from os import environ
from os.path import dirname, exists, join


def get_config():
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    def_pth = join(dirname(__file__), "model.cfg")
    config_pth = environ.get("MODEL_CONFIG", def_pth)
    if exists(config_pth):
        config.read(config_pth)
        return config
    else:
        raise FileNotFoundError("No Config path found")
