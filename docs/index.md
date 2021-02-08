# DBgen

<p align="center">
  <a href="https://dbgen.modelyst.com"><img src="img/dbgen_logo.png" alt="DBgen"></a>
</p>
---

**Documentation**: <a href="https://dbgen.modelyst.com" target="_blank">https://dbgen.modelyst.com</a>

**Github**: <a href="https://github.com/modelyst/dbgen" target="_blank">https://github.com/modelyst/dbgen</a>

---

DBgen (Database Generator) is an open-source Python library for
connecting raw data, scientific theories, and relational databases.
These are some of the main features:

1.  Very easy to work with
2.  Integration with the PostGres databases.

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

DBGen is an opinionated ETL tool. ETL tools exist but they rarely
give the tools necessary for a scientific workflow. Opinionated
aspect: it really cares about what the end product is (ID columns on
all the tables). We're dealing with a much more restricted ETL
problem (extracting and ).

Comparison to

1. [Airflow](https://airflow.apache.org/)
    -   Has a priority for ETL scalability

2. [Fireworks](https://materialsproject.github.io/fireworks/)


3. [AiiDA](http://www.aiida.net/) or [Atomate](https://atomate.org/)
    -   We don't focus on the actual submission of computational
        science workflows.

## What isn't DBgen?

1. An [ORM](https://en.wikipedia.org/wiki/Object-relational_mapping) tool (see [Hibernate](http://hibernate.org/orm/) for Java or [SQLAlchemy](https://www.sqlalchemy.org/) for Python)
    - DBgen operates at a higher level of abstrload, not exposing the user to low level SQL commands like SELECT or INSERT.

2. A database manager (see
   [MySQLWorkbench](https://www.mysql.com/products/workbench/),
   [DBeaver](https://dbeaver.io/), [TablePlus](https://tableplus.com/),
   etc.)
3. An opinioniated tool with a particular schema for scientific data /
   theories.

## Getting DBgen

### Via pip

The easiest way to install the latest stable version of DBgen is to use
pip:

    pip install dbgen

(Getting development version? If not, then just merge the above into
Quick Start)

### Quick start

<div class="termy">

```console
$ pip install uvicorn[standard]

---> 100%
```

</div>



### API documentation

Documentation of modules and classes can be found in
API docs \</modules\>.

#### Reporting bugs

Please report any bugs and issues at DBgen's [Github Issues
page](https://github.com/modelyst/dbgen/issues).

## License

DBgen is released under the [Apache 2.0 License](license/).
