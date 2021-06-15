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
