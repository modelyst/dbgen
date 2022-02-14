<!--
   Copyright 2021 Modelyst LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 -->

# DBgen

<p align="center">
  <a href="https://dbgen.modelyst.com"><img src="docs/img/dbgen_logo.png" alt="DBgen"></a>
</p>

<p align="center">
   <a href="https://github.com/modelyst/dbgen   /actions?query=workflow%3ATest" target="_blank">
      <img src="https://github.com/modelyst/dbgen/workflows/Test/badge.svg" alt="Test">
   </a>
   <a href="https://github.com/modelyst/dbgen/actions?query=workflow%3APublish" target="_blank">
      <img src="https://github.com/modelyst/dbgen/workflows/Publish/badge.svg" alt="Publish">
   </a>
   <a href="https://github.com/modelyst/dbgen/actions/workflows/publish_docs.yml" target="_blank">
      <img src="https://github.com/modelyst/dbgen/actions/workflows/publish_docs.yml/badge.svg">
   </a>
   <a href="https://codecov.io/gh/modelyst/dbgen">
      <img src="https://codecov.io/gh/modelyst/dbgen/branch/master/graph/badge.svg?token=V4I8PPUIBU"/>
   </a>
   <a href="https://codecov.io/gh/modelyst/dbgen">
      <img src="docs/img/interrogate.svg"/>
   </a>
   <a href="https://pypi.org/project/modelyst-dbgen" target="_blank">
      <img src="https://img.shields.io/pypi/v/modelyst-dbgen?color=%2334D058&label=pypi%20package" alt="Package version">
   </a>
</p>
---

**Documentation**: <a href="https://dbgen.modelyst.com" target="_blank">https://dbgen.modelyst.com</a>

**Github**: <a href="https://github.com/modelyst/dbgen" target="_blank">https://github.com/modelyst/dbgen</a>

---

:exclamation:  Please note that this project is actively under major rewrites and installations are subject to breaking changes.

---
DBgen (Database Generator) is an open-source Python library for
connecting raw data, scientific theories, and relational databases.
The package was designed with a focus on the developer experience at the core.
DBgen was initially developed by [Modelyst](https://www.modelyst.com/).

## What is DBgen?

DBgen was designed to support scientific data analysis with the following
characteristics:

1.  Transparent

    - Because scientific efforts ought be shareable and mutually
      understandable.

2.  Flexible

    - Because scientific theories are under continuous flux.

3.  Maintainable
    - Because the underlying scientific models one works with are
      complicated enough on their own, we can't afford to introduce
      any more complexity via our framework.

DBGen is an opinionated ETL tool. While many other ETL tools exist, they rarely
give the tools necessary for a scientific workflow.
DBGen is a tool that helps populate a single postgresql database using a transparent, flexible, and mainatable data pipeline.

### Alternative tools

Orchestrators: Many tools exist to orchestrate python workflows. However, these tools often often are too general to help the average scientist wrangle their data or are so specific to storing a given workflow type they lack the flexibility needed to address the specifics of a scientist's data problems. Many other tools also come packaged with powerful
#### General Orchestration Tools
1. [Airflow](https://airflow.apache.org/)
2. [Prefect](https://www.prefect.io/)
3. [Luigi](https://github.com/spotify/luigi)

#### Computational Science Workflow Tools
1. [Fireworks](https://materialsproject.github.io/fireworks/)
2. [AiiDA](http://www.aiida.net/)
3. [Atomate](https://atomate.org/)

## What isn't DBgen?

1. An [ORM](https://en.wikipedia.org/wiki/Object-relational_mapping) tool (see [Hibernate](http://hibernate.org/orm/) for Java or [SQLAlchemy](https://www.sqlalchemy.org/) for Python)

   - DBGen utilizes the popular SQLAlchemy ORM to operate at an even higher level extraction, allowing the users to build pipelines and schema without actively thinking about the database tables or insert and select statements required to connect the workflow together.

2. A database manager (see
   [MySQLWorkbench](https://www.mysql.com/products/workbench/),
   [DBeaver](https://dbeaver.io/), [TablePlus](https://tableplus.com/),
   etc.)
3. An opinionated tool with a particular schema for scientific data /
   theories.

## Getting DBgen

### Via Github

Currently, the only method of installing DBgen is through Github. This is best done by using the [poetry](https://python-poetry.org/) package manager. To do this, first clone the repo to a local directory. Then use the command `poetry install` in the directory to install the required dependencies. You will need at least python 3.7 to install the package.
```Bash
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
```
### Via Pip
```Bash
pip install modelyst-dbgen
```

### API documentation

Documentation of modules and classes can be found in
API docs \</modules\>.

#### Reporting bugs

Please report any bugs and issues at DBgen's [Github Issues
page](https://github.com/modelyst/dbgen/issues).

## License

DBgen is released under the [Apache 2.0 License](license/).
