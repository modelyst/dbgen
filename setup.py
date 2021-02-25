"""Installation specifications for dbgen"""
# External imports
import unittest
from os.path import dirname, join
import logging


from setuptools import find_packages, setup  # type: ignore

logger = logging.getLogger(__name__)
DBGEN_DIR = dirname(__file__)
version = "0.4.1"
# Set long description as readme file text
try:
    with open(join(DBGEN_DIR, "README.md"), encoding="utf-8") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""


def get_git_version(version_: str):
    """
    Writes the current git version to git_version if this is a git repo

    Args:
        version_ (str): the semantic version to prepend to file

    Returns:
        str: the full version with git
    """
    try:
        import git  # type: ignore

        try:
            repo = git.Repo(join(*[DBGEN_DIR, ".git"]))
        except git.NoSuchPathError:
            logger.warning(".git directory not found: Cannot compute the git version")
            return ""
        except git.InvalidGitRepositoryError:
            logger.warning(
                "Invalid .git directory not found: Cannot compute the git version"
            )
            return ""
    except ImportError:
        logger.warning("gitpython not found: Cannot compute the git version.")
        return ""
    if repo:
        sha = repo.head.commit.hexsha
        if repo.is_dirty():
            return f".dev0+{sha}.dirty"
        # commit is clean
        return f".release:{version_}+{sha}"
    return "no_git_version"


def write_version():
    full_version = get_git_version(version)
    with open(join(DBGEN_DIR, "dbgen", "git_version"), "w") as f:
        f.write(full_version)


def dbgen_tests():
    """Test suite for Dbgen tests"""
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(join(DBGEN_DIR, "tests"), pattern="test_*.py")
    return test_suite


# Requirements
docs = [
    "mkdocs",
    "mkdocs-material",
    "mkdocs-markdownextradata-plugin",
    "markdown-include",
    "mkdocstrings",
]

INSTALL_REQUIREMENTS = [
    "hypothesis>=5.23.7",
    "infinite>=0.1",
    "jinja2>=2.10.0",
    "networkx>=2.4",
    "pathos>=0.2.6",
    "psycopg2-binary>=2.7.4",
    "sshtunnel==0.1.5",
    "tqdm>=4.48.0",
]

EXTRAS_REQUIREMENTS = {"airflow": ["apache-airflow"], "docs": docs}


def do_setup():
    """Perform the DBgen package setup."""
    setup(
        name="dbgen",
        description="Tool for defining complex schema and ETL pipelines",
        long_description=long_description,
        long_description_content_type="text/markdown",
        license="Apache License 2.0",
        version=version,
        packages=find_packages(exclude=["tests*"]),
        package_data={"dbgen.templates": ["*.jinja"]},
        include_package_data=True,
        install_requires=INSTALL_REQUIREMENTS,
        setup_requires=["gitpython", "setuptools", "wheel"],
        extras_require=EXTRAS_REQUIREMENTS,
        classifiers=[
            "Environment :: Console",
            "Intended Audience :: Scientists",
            "Intended Audience :: Developers",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
        ],
        author="Modelyst LLC",
        author_email="info@modelyst.io",
        url="http://www.modelyst.com",
        python_requires="~=3.6",
        test_suite="setup.dbgen_tests",
    )


if __name__ == "__main__":
    do_setup()
