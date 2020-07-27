
.. image:: https://cdn.iconscout.com/icon/premium/png-512-thumb/database-997-801373.png

============
Introduction
============

DBgen (Database Generator) is an open-source Python library for connecting raw data, scientific theories, and relational databases. These are some of the main features:

1. ???
2. Integration with the PostGres databases.


DBgen was initially developed by `Modelyst <https://www.modelyst.io/>`_ .

What is DBgen?
==============

DBgen was designed to support scientfic data analysis with the following characteristics:

1. Transparent
 - Because scientific efforts ought be shareable and mutually understandable.
 - Achieved by ???
2. Flexible
 - Because scientific theories are under continuous flux.
 - Achieved by ???
3. Maintainable
 - Because the underlying scientific models one works with are complicated enough on their own, we can't afford to introduce any more complexity via our framework.
 - Achieved by??

 DBGen is an opinionated ETL tool. ETL tools exist but they rarely give the tools necessary for a scientific workflow. Opinionated aspect: it really cares about what the end product is (ID columns on all the tables). We're dealing with a much more restricted ETL problem (extracting and ).

 Comparison to

 1. `Airflow <https://airflow.apache.org/>`_
  - Has a priority for ETL scalability
 2. `Fireworks <https://materialsproject.github.io/fireworks/>`_
  - ???
 3. `AiiDA <http://www.aiida.net/>`_ or `Atomate <https://atomate.org/>`_
  - We don't focus on the actual submission of computational science workflows.

What isn't DBgen?
=================
1. An `ORM <https://en.wikipedia.org/wiki/Object-relational_mapping>`_ tool (see `Hibernate <http://hibernate.org/orm/>`_ for Java or  `SQLAlchemy <https://www.sqlalchemy.org/>`_ for Python)
 - DBgen operates at a higher level of abstraction, not exposing the user to low level SQL commands like :code:`SELECT` or :code:`INSERT`.
 - DBgen behaves
2. A database manager (see `MySQLWorkbench <https://www.mysql.com/products/workbench/>`_, `DBeaver <https://dbeaver.io/>`_, `TablePlus <https://tableplus.com/>`_, etc.)
3. An opinioniated tool with a particular schema for scientific data / theories.

Getting DBgen
================

Via pip
~~~~~~~

The easiest way to install the latest stable version of DBgen is to use pip::

    pip install dbgen

(Getting development version? If not, then just merge the above into Quick Start)


.. _quick_start:

Quick start
~~~~~~~~~~~

.. code-block:: pycon

    >>> import dbgen
    >>>


API documentation
~~~~~~~~~~~~~~~~~

Documentation of modules and classes can be found in :doc:`API docs </modules>`.

Reporting bugs
--------------

Please report any bugs and issues at DBgen's
`Github Issues page <https://github.com/modelyst/dbgen/issues>`_.

License
=======

DBgen is released under the ??? License. The terms of the license are as
follows:

.. literalinclude:: ../LICENSE.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

