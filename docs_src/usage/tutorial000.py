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

solarcell = Entity(
    name='solarcell',
    desc='a solar cell',
    attrs=[
        Attr('id', Int(), desc='Identifying number', identifying=True),
        Attr('frac_La', Decimal(), desc='frload Lanthanum'),
        Attr('frac_Co', Decimal(), desc='frload Cobalt'),
    ],
)

jvcurve = Entity(
    name='jvcurve',
    desc='A curve describing the current density vs voltage behavior of a solar cell',
    attrs=[
        # Attr('id',Int(),desc='Identifying number', id=True),
        Attr('full_path', Varchar(), desc='Full path to the jv curve', identifying=True),
        Attr('voc', Decimal(), desc='Open circuit voltage'),
        Attr('jsc', Decimal(), desc='Short circuit current density'),
        Attr('max_power_v', Decimal(), desc='Voltage of maximum power point'),
        Attr('max_power_j', Decimal(), desc='Current density of maximum power point'),
        Attr('fill_factor', Decimal(), desc='Fill factor'),
    ],
    fks=[Rel('solarcell')],
)

#####################################################################

objs = [solarcell, jvcurve]


def make_model() -> Model:
    m = Model('test_db')
    m.add(objs)
    add_generators(m)
    return m
