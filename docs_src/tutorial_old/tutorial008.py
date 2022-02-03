@transform(env=Env(Import('numpy', lib_alias='np')), outputs=['voc', 'jsc'])
def parse_jv_csv(file_path: str) -> tuple[float, float]:
    jv_arr = np.genfromtxt(file_path, delimiter=',', skip_header=1, dtype=float)
    nrows, *_ = np.shape(jv_arr)
    for row in range(nrows):
        if jv_arr[row][0] > 0:
            jsc = jv_arr[row][1]
            break

    for row in range(nrows):
        if jv_arr[row][1] > 0:
            voc = jv_arr[row][0]
            break

    return voc, jsc


with Generator('load_jv_curves'):
    extract = LocalCSVExtract(data_dir=os.environ['DATA_DIR'])
    file_path = extract['file_path']
    voc, jsc = parse_jv_csv(file_path).results()

    JVCurve.load(
        insert=True, full_path=file_path, open_circuit_voltage=voc, short_circuit_current_density=jsc
    )
