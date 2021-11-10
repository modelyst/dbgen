from dbgen import Model

with Model(name="example") as model:
    with Generator('load_jv_curves'):
        extract = LocalCSVExtract(data_dir=os.environ['DATA_DIR'], outputs=['file_path'])
        ...
        ...
