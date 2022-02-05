## Introduction


In this tutorial, we walk through a series of lessons that demonstrate the usage of DBgen's core functionality.

In this tutorial, Alice and Bob run a lab in which researchers make temperature measurements. First, we'll add the researchers' names into the database from a CSV. Next, we'll show how to use custom parsers to import temperature data stored in a directory of text files. Finally, we'll show a very simple analysis of the data: converting the temperature from F to C.


## Models

A Model is the conceptually "largest" object in DBgen. A Model contains all of the information required to build a database. When defining a Model, there are two key steps:

- Define the tables and columns that will exist in your database.
- Define the procedure for populating those tables with actual data.

In order to accomplish the first step, we need to understand DBgen "Entities," and in order to accomplish the second step, we need to understand DBgen "Extracts", Transforms", Loads", and "Generators". These concepts are introduced below.

## Entities

Using DBgen, entities are defined by subclassing dbgen.core.entity.Entity with
table=True.

Let's define a table called Person to store information about Alice and Bob's
researchers. In the example below, we define the Person table with just three
attributes: first_name, last_name, and age.


DBgen Entity is a subclass of Pydantic's SQLModel. Therefore, the syntax for adding
attributes to DBgen entities is the same as adding properties to Pydantic
SQLModels (attribute_name: data_type).

Additionally, we can give the table a name that will be used when creating the
database by setting the `__tablename__` attribute. This way, the name of your
python class and your database table don't need to be the same.

We can make columns optional (null values allowed) by wrapping the data type
with `Optional[]`, where `Optional` is imported from python's `typing` built-in
library.

```python3 
{!../docs_src/tutorial/tutorial000.py!}
```


### Identifying Attributes
Finally, you'll notice that we have set an attribute called `__identifying__`.

The distinction between identifying and non-identifying attributes is important
in DBgen. The identifying attributes answer the question, "what defines one row
in this table?" The identifying information is the minimum set of information
that is guaranteed to return one row. In more technical terms, the primary key
for each row in a table created using DBgen is a hash of that row's identifying
attributes. In this case, we have specified that a row in this table fully identified by the person's first and last name. With this design decision, it is impossible for us to insert two people with the same first and last name into the database. 


### Adding foreign keys

Foreign keys specify the relationships between the entities in the schema. For
example, in this example, the same person may make many temperature
measurements. Let's define a table to capture temperature measurements, which
order they were captured in, and who recorded them.

```python3
{!../docs_src/tutorial/tutorial001.py!}
```

