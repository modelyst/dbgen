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
