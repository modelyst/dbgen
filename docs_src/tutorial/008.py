with Generator('load_jv_curves'):
    extract = LocalCSVExtract(data_dir=os.environ['DATA_DIR'], outputs=['file_path'])
    file_path = extract['file_path']

    @transform(env=Env([Import('numpy')]), outputs=['voc', 'jsc'])
    def parse_jv_csv(file_path: str) -> tuple[float, float]:
        jv_arr = numpy.genfromtxt(file_path, delimiter=',', skip_header=1, dtype=float)
        for row in range(numpy.shape[0]):
            if jv_arr[row][0] > 0:
                jsc = jv_arr[row][1]
                break

        for row in range(numpy.shape[0]):
            if jv_arr[row][1] > 0:
                voc = jv_arr[row][0]
                break

        return voc, jsc

    voc, jsc = parse_jv_csv(file_path).results()

    JVCurve.load(
        insert=True, full_path=file_path, open_circuit_voltage=voc, short_circuit_current_density=jsc
    )
