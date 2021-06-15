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


def jsc(model: Model) -> None:

    # Get tables
    JVcurve = model.get('jvcurve')

    ###########################################################################

    query = Query(exprs={'full_path': JVcurve['full_path'](), 'jvcurve_id': JVcurve.id()})

    ###########################################################################

    get_jsc_block = PyBlock(
        get_jsc,
        env=defaultEnv + Env([Import('os'), Import('numpy as np')]),
        args=[query['full_path']],
        outnames=['jsc'],
    )

    get_jsc_generator = Gen(
        name='get_jsc',
        desc='finds the JSC',
        query=query,
        transforms=[get_jsc_block],
        tags=['pure'],
        loads=[JVcurve(jvcurve=query['jvcurve_id'], jsc=get_jsc_block['jsc'])],
    )

    ###########################################################################
    gens = [get_jsc_generator]
    model.add(gens)
