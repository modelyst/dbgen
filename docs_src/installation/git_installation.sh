# Get DBgen
git clone https://github.com/modelyst/dbgen
cd ./dbgen
# Get Poetry
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 -
# Install Poetrywhich ma
poetry install
poetry shell
# Test dbgen
dbgen serialize dbgen.example.main:make_model
