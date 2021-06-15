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


def io(model: Model) -> None:

    # Get tables
    entities = ['jvcurve']

    JVcurve = model.get('jvcurve')

    ###########################################################################

    load_data_paths_block = PyBlock(
        load_data_paths,
        env=defaultEnv + Env([Import('os')]),
        args=[Const(join(root, 'data/jvcurves'))],
        outnames=['full_path'],
    )

    load_paths_generator = Gen(
        name='load_data_paths',
        desc='loads the full path to all jvcurves',
        funcs=[load_data_paths_block],
        tags=['io'],
        loads=[JVcurve(insert=True, full_path=load_data_paths_block['full_path'])],
    )

    ###########################################################################
    gens = [load_paths_generator]
    model.add(gens)
