from hypothesis import strategies as st, given, example
from typer import BadParameter
from typer.testing import CliRunner
import pytest
from dbgen.cli.main import (
    app,
    validate_model_str,
    ERROR_FORMAT,
    ERROR_MODULE,
    ERROR_NOT_MODEL,
    ERROR_NOT_MODEL_FUNCTION,
    ERROR_RUNNING_MODEL_FACT,
)
from dbgen.core.model.model import Model

runner = CliRunner()

# Trim Errors for regex matching
trim_error = lambda x: x.split(":")[0]
ERROR_FORMAT = trim_error(ERROR_FORMAT)
ERROR_MODULE = trim_error(ERROR_MODULE)
ERROR_NOT_MODEL = trim_error(ERROR_NOT_MODEL)
ERROR_NOT_MODEL_FUNCTION = trim_error(ERROR_NOT_MODEL_FUNCTION)
ERROR_RUNNING_MODEL_FACT = trim_error(ERROR_RUNNING_MODEL_FACT)

no_colons = st.text(st.characters(whitelist_categories=("Lu", "Ll"), blacklist_characters=(":")), min_size=1,)


@given(no_colons, no_colons)
def test_import(model, package):
    result = runner.invoke(app, ["run", ":".join([model, package])])
    assert result.exit_code == 2


def test_empty_string():
    """tests when empty strings are fed to model paramater"""
    with pytest.raises(BadParameter, match=ERROR_FORMAT):
        validate_model_str(":")
    with pytest.raises(BadParameter, match=ERROR_FORMAT):
        validate_model_str("")


@given(no_colons, no_colons)
@example("dbgen", "core")
def test_basic_import_notfound(model, package):
    """Tests a simple model package validation"""
    try:
        __import__(model, globals(), locals(), [package])
    except ImportError:
        with pytest.raises(BadParameter, match=ERROR_MODULE):
            validate_model_str(":".join([model, package]))


@pytest.mark.parametrize("model,package", [("dbgen.example.main", "model")])
def test_basic_import_found(model, package):
    """Tests a simple model package validation"""
    __import__(model, globals(), locals(), [package])
    model = validate_model_str(":".join([model, package]))
    assert isinstance(model, Model)


not_models = [("dbgen", "core"), ("typing", "List")]


@pytest.mark.parametrize("model,package", not_models)
def test_basic_import_not_a_model(model, package):
    """Tests when import string is valid but is not a dbgen Model"""
    __import__(model, globals(), locals(), [package])
    with pytest.raises(BadParameter, match=ERROR_NOT_MODEL):
        model = validate_model_str(":".join([model, package]))
        assert isinstance(model, Model)
