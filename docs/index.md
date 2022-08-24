<!--
   Copyright 2022 Modelyst LLC

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
  <a href="https://dbgen.modelyst.com"><img src="img/dbgen_logo.png" alt="DBgen"></a>
</p>

<p align="center">
   <a href="https://github.com/modelyst/dbgen/actions?query=workflow%3ATest" target="_blank">
      <img src="https://github.com/modelyst/dbgen/workflows/Test/badge.svg" alt="Test">
   </a>
   <a href="https://github.com/modelyst/dbgen/actions?query=workflow%3APublish" target="_blank">
      <img src="https://github.com/modelyst/dbgen/workflows/Publish/badge.svg" alt="Publish">
   </a>
   <a href="https://github.com/modelyst/dbgen/actions/workflows/publish_docs.yml" target="_blank">
      <img src="https://github.com/modelyst/dbgen/actions/workflows/publish_docs.yml/badge.svg">
   </a>
</p>
<p align="center">
   <a href="https://codecov.io/gh/modelyst/dbgen">
      <img src="https://codecov.io/gh/modelyst/dbgen/branch/master/graph/badge.svg?token=V4I8PPUIBU"/>
   </a>
   <a href="/status">
      <img src="img/interrogate.svg"/>
   </a>
   <a href="https://pypi.org/project/modelyst-dbgen" target="_blank">
      <img src="https://img.shields.io/pypi/v/modelyst-dbgen?color=%2334D058&label=pypi%20package" alt="Package version">
   </a>
   <a href="https://github.com/modelyst/dbgen/actions/workflows/docker-publish.yml" target="_blank">
      <img src="https://github.com/modelyst/dbgen/actions/workflows/docker-publish.yml/badge.svg">
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

Many tools exist to orchestrate python workflows. However, these tools often often are too general to help the average scientist wrangle their data or are so specific to storing a given computational workflow type they lack the flexibility needed to address the specifics of a scientist's data problems. Many other tools also come packaged with powerful yet complex scheduling systems (such as airflow and prefect) that can be quite complex to setup and can make the initial development very difficult for scientists without extensive devops experience.
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

   - DBGen utilizes the popular SQLAlchemy ORM to operate at an even higher level of abstraction, allowing the users to build pipelines and schema without actively thinking about the database tables or SQL insert and select statements required to populate the database.

2. A database manager (see
   [MySQLWorkbench](https://www.mysql.com/products/workbench/),
   [DBeaver](https://dbeaver.io/), [TablePlus](https://tableplus.com/),
   etc.)
3. An tool that can only be used with specific schemas.
