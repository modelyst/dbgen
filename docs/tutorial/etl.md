# Refining Data in the Database

When working with scientific data, it is very common to pass data through a series of data analysis steps. The nature of these analysis steps are highly domain-specific. To avoid any domain-specific jargon, in this tutorial, we walk through a very simple data analysis method: converting Fahrenheit to Celsius.

In practice, the majority of generators are of this type.

<!-- In signal processing, it may be common to take Fourier Transforms, in spectroscopy, it may be common to do background subtraction and peak-finding.  -->

# Defining the Generator

In order to carry out data analysis like this, we need to define a new "extract, transform, load" (ETL) step, which means we need to define a new DBgen Generator.

In this case, instead of using a custom extract class to define the data source, we will use a query against the database. The transform and load steps are similar to what you have already seen.

## Setup

As always, we begin by adding a new file to the `generators` module and define a function that accepts the model as an input then begins with `with model:` then `with Generator(name=...):`

```python3
{!../docs_src/tutorials/alice_bob_lab/generators/f_to_c.py [ln:18-20] !}
```

## Query

Instead of using a custom extract, we will use a `Query` as the data source because the required input data is already in the database.

DBgen uses sqlalchemy for querying. The query syntax is documented in detail <a href="https://www.sqlalchemy.org/">here</a>.

To do this, first, `Query` must be imported from `dbgen`, and `select` must be imported from `sqlmodel`.

For queries that include only one table (which is the case in this example), the syntax is simply: `select(TableName.ColumnName_1, TableName.ColumnName_2)`.

Similarly to the extracts shown in the previous generators, by calling `.results()` on the `Query`, a tuple of the outputs are returned in the order that they are listed in the `select` statement.

```python3 hl_lines="11-13"
{!../docs_src/tutorials/alice_bob_lab/generators/f_to_c.py [ln:1-2,4,18-] !}
```

## Imports and Environments

In this example, although it is quite simple to write a function to convert F to C, we will import and use scipy's `convert_temperature` function in order to show how to use DBgen with python environments that include non-built-in libraries like scipy.

The `@transform` decorator specifies two things:

- The output names (a list of strings the same length as the number of the function's outputs)
- The python environment (a `dbgen.Env` object)

To define a `dbgen.Env` object, we start by importing `Env` and `Import` from dbgen.

To create an `Env` object, we pass a list of `Imports` to `Env()`. To create an `Import`, we pass two arguments to `Import()`:

- The name of the python module that we would like to import a function or class from (a string)
- The name of that function or class (a string)

It is also possible to call `Import()` with just the first argument to import the entire module.

Essentially, whenever a line like...

```from library import function```

...appears above a function definition, we need to include...

```Import("library", "function")```

...in the list of imports in the dbgen `Env`.

Finally, it is worth noting that dbgen `Env` objects can be added together. So, if there is a default `Env` that is used for most transforms, and we just need to add one extra import to that, rather than define a new `Env` from scratch that includes every package, we can simply create an `Env` with just the one new import and add it to the default `Env`. An example of doing just that is shown below.

```python3 hl_lines="8"
{!../docs_src/tutorials/alice_bob_lab/generators/f_to_c.py [ln:1-] !}
```


## Transform

Once the environment is defined, the transform step is very similar to the previously-shown transform steps. We define a function that carries out the desired data analysis and add the `@transform` decorator to specify the output names and the python environment.

```python3 hl_lines="7-15"
{!../docs_src/tutorials/alice_bob_lab/generators/f_to_c.py [ln:1-] !}
```

## Load

The load step is very similar to what has been shown in the previous generators. However, in this case, rows are being updated, not added.

As always, `TableName.load()` is called, and keyword arguments are passed where the keywords are the names of the columns, as specified in `schema.py`, and the arguments are the names of the corresponding variables defined in the generator by calling `.results()` on `Extracts`, `Queries`, or `transforms`.

And, as always, when `.load()` is called, we must supply either:

- keyword arguments for each of the identifying columns (as specified in `schema.py`, or
- the keyword `[tablename]` and the corresponding id for the row). This id either comes from:
    - querying for the `.id` column (as shown in this example), or 
    - calling `Tablename.load()`, which always returns the id of the specified row (as shown in the previous generator).


```python3 hl_lines="25-28"
{!../docs_src/tutorials/alice_bob_lab/generators/f_to_c.py [ln:1-] !}
```