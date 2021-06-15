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

from dbgen import Env, Gen, Import, Model, PyBlock, Query


def fill_factor(model: Model) -> None:

    # Get tables
    JVcurve = model.get('jvcurve')

    #######################################################################
    ### Query

    query = Query(
        exprs={
            'jvcurve_id': JVcurve.id(),
            'voc': JVcurve['voc'](),
            'jsc': JVcurve['jsc'](),
            'max_power_v': JVcurve['max_power_v'](),
            'max_power_j': JVcurve['max_power_j'](),
        }
    )

    #######################################################################

    get_ff_block = PyBlock(
        get_fill_factor,
        env=defaultEnv,
        args=[query['voc'], query['jsc'], query['max_power_v'], query['max_power_j']],
        outnames=['ff'],
    )

    get_ff_generator = Gen(
        name='get_fill_factor',
        desc='finds the VOC',
        query=query,
        transforms=[get_ff_block],
        tags=['pure'],
        loads=[JVcurve(jvcurve=query['jvcurve_id'], fill_factor=get_ff_block['ff'])],
    )

    #######################################################################
    gens = [get_ff_generator]
    model.add(gens)
