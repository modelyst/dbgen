"""Main function for the dbgen testing"""
from os.path import dirname
import unittest

TEST_DIR = dirname(__file__)


def get_dbgen_test_suite() -> unittest.TestSuite:
    """Load the tests and return the test suit)"""

    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(TEST_DIR, pattern="test_*.py")
    return test_suite


if __name__ == "__main__":
    test_suite = get_dbgen_test_suite()
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
