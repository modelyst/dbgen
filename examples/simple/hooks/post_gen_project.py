import os
import shutil

REMOVE_PATHS = [
    '{% if cookiecutter.clean=="true" %} {{cookiecutter.model_name}}/generators/io.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} {{cookiecutter.model_name}}/scripts/io.py {% endif %}',
    '{% if cookiecutter.clean=="true" %} data/ {% endif %}',
]

for path in REMOVE_PATHS:
    path = path.strip()
    if path and os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)

# change name of env
shutil.move('temp.env', '.env')
