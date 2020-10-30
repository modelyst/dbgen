import pytest


params = ["one", "two", "three"]


@pytest.fixture
def test_fixture():
    print("Starting fixture")
    yield "Fixture"
    print("Tearing Down Fixture")


@pytest.fixture
def test_simple(test_fixture):
    print(test_fixture)
    return "Second Fixture"


def test_simpler(test_simple):
    print(test_simple)
