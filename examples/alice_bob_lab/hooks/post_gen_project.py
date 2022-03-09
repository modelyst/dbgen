import os
import shutil
from textwrap import dedent

REMOVE_PATHS = [
    '{% if cookiecutter.clean=="true" %} alice_bob_model/etl_steps/f_to_c.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} alice_bob_model/etl_steps/parse_measurements.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} alice_bob_model/etl_steps/read_csv.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} alice_bob_model/etl_steps/__init__.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} alice_bob_model/extracts/csv_extract.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} alice_bob_model/extracts/measurement_extract.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} alice_bob_model/schema.py {% endif %}',
]

text_to_write = {
    'alice_bob_model/etl_steps/__init__.py': """
    from dbgen import Model

    def add_etl_steps(model: Model):
        # Add etl_steps here
        pass
    """
}

for path in REMOVE_PATHS:
    path = path.strip()
    if path and os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)
            # Touch the files to create empty versions
            with open(path, 'w') as f:
                new_text = dedent(text_to_write.get(path, '')).strip()
                f.write(new_text)


# change name of env
shutil.move('temp.env', '.env')
