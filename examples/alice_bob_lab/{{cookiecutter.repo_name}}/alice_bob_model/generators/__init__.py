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

from alice_bob_model.f_to_c import add_f_to_c
from alice_bob_model.parse_measurements import add_temperature_generator
from alice_bob_model.read_csv import add_io_generator

from dbgen import Model


def add_generators(model: Model) -> None:
    add_io_generator(model)
    add_temperature_generator(model)
    add_f_to_c(model)
