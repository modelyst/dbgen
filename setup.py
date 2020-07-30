# External imports
from os.path import join, dirname
from setuptools import setup, find_packages  # type: ignore

DBGEN_DIR = dirname(__file__)

# Set long description as readme file text
try:
    with open(join(DBGEN_DIR, "README.md"), encoding="utf-8") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""

# Requirements
docs = [
    "mkdocs",
    "mkdocs-material",
    "mkdocs-markdownextradata-plugin",
    "markdown-include",
]

with open("requirements.txt", "r") as f:
    INSTALL_REQUIREMENTS = [line.strip() for line in f.readlines()]


EXTRAS_REQUIREMENTS = {"airflow": ["apache-airflow"], "docs": docs}


def do_setup():
    """Perform the DBgen package setup."""
    setup(
        name="dbgen",
        description="Tool for defining complex schema and ETL pipelines",
        long_description=long_description,
        long_description_content_type="text/markdown",
        license="Apache License 2.0",
        version="0.2.0",
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
        url="http://modelyst.io",
        python_requires="~=3.6",
    )


if __name__ == "__main__":
    do_setup()
