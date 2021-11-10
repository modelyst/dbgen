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

# Usage

This page walks through an extended example which can expose new users
to elements of the DBgen code base.

## The scenario

![image](img/jvcurve.png)

> Consider a materials science example of analyzing experimental solar
> cell data.

### Schema Graph vs Fact Graph

The scientist's knowledge is formalized in two data structures, a
_schema graph_ and an _ETL graph_. The schema graph declares what
entities exist and what relations hold between them. This structure maps
neatly onto the notion of a schema for a relational database, and a
database with this structure (populated with the correct contents) is
the end result of running a DBgen model. In this example, our entities
are J-V curves (the result of an experiment) and material samples. Note
the edge sample_id indicates a many-to-one relationship, i.e. each
material could have multiple J-V curve-measuring experiments done on it.
The process of constructing this via DBgen will be discussed further
below.

![image](img/schemagraph.png)

> The ETL graph is defined in reference to the schema graph and concerns
> specifying the process by which the schema is populated with instance
> data. The figure below shows an example fact; at a high level, the
> relevant metric to derive from the raw experimental data is a _fill
> factor_, and this metric is itself computed from three independent facts
> derivable from an experiment's raw data. Again, the code that can
> specify this process will be demonstrated below.

![image](img/factgraph.png)

## Defining the Schema Graph

The schema is specified by providing a list of entities. Each entity represents
one table. Entities have attributes (the columns of the table)and foreign keys
to other entitesLet's walk through defining entities, attributes, and foreign
keys one step at a time.

# Defining Entities
Using DBgen, entities are defined by subclassing dbgen.core.entity.EntityID with
table=True.

```python3
{!../docs_src/tutorial/000.py!}
```

This defines a database table called SimplestPossibleTable, which has no columns
and is therefore not very useful in any practical application.

# Adding Attributes
DBgen EntityID's subclass Pydantic SQLModels. Therefore, the syntax for adding
attributes to DBgen entities is the same as adding properties to Pydantic
SQLModels (attribute_name: data_type). In the example below, we define the
Sample table from the JV-curve example by adding attributes.

```python3
{!../docs_src/tutorial/001.py!}
```

# Identifying Attributes
The distinction between identifying and non-identifying attributes is important
in DBgen. The identifying attributes answer the question, "what defines one row
in this table?" The identifying information is the minimum set of information
that is guaranteed to return one row. In more technical terms, the primary key
for each row in a table created using DBgen is a hash of that row's identifying
attributes. For example, if we defined a "Person" table, we could choose to make
the following columns identifying: first name, last name, birthday, and
hometown. This design choice would make it impossible for there to be two rows
in the person table with the same first name, last name, and birthday. We could
instead choose to make social security number (SSN) the one identifying
attribute. This design choice would allow for multiple rows with the same first
name, last name, and birthday, but it would require a person's SSN to be known
in order to insert rows into the database.

Non-identifying attributes are simply additional information that may or may not
be known for a given row and may or may not be added to a row after the time the
row is created. For example, we may add height and favorite food as
non-identifying columns to our person table.

In DBgen, the identifying attributes are specified by setting the
\__identifying__ attribute in the table's class definition, as shown below:

```python3
{!../docs_src/tutorial/002.py!}
```

# Adding foreign keys

Foreign keys specify the relationships between the entities in the schema. For
example, in this tutorial's example, many J-V curves can be measured using the
same solar cell sample. Therefore, we should add a foreign key from the JVCurve
table to the Sample table. The syntax for doing that is shown below.

```python3
{!../docs_src/tutorial/003.py!}
```

# Inheritance
Notice that both Samples and JVCurves have  `created` and `created_by` as
attributes. Instead of defining these twice, we can instead define a base class
that both the Sample and the JVCurve class inherit from. Since the base class is
not a table in the database, it is defined as a DBGen EntityID with table=False.
The syntax for doing this is shown below.

```python3
{!../docs_src/tutorial/004.py!}
```

## Defining the ETL Graph

Each node in the ETL graph is called a Generator and has a similar structure:
Extract, Transform, Load. We'll go through these step-by-step

# Extracts

The source of the data for a given node in the ETL graph could be either the
database itself or an outside source. When the source of the data is outside the
database, defining a custom Extract is useful. To do this, we sublcass the dbgen
`Extract` class and overwrite the extract method. This method should be a python
generator, so `yield` should be used rather than `return`. Optionally, we can
overwrite the `length` method so that the custom extract can tell DBgen how many
items it expects to yield. This enables features like the progress bar.
Additionally, if there is some initial setup or final teardown that needs to be
done in the extract, the `setup` and `teardown` methods can be overwritten.
`setup` is always run before `extract` or `length` are called, and `teardown` is
run at the end (`extract` and `length` are never called after `teardown`). So,
internal variables can be set in the `setup` and safely be used in the
`extract`. It is important to note that extracts must yield dictionaries. This
is so that the outputs are named (by their keys) and DBgen will know what data
to pass as arguments to other nodes in the ETL graph. Below is a very simple
custom extract that gets CSV files.


```python3
{!../docs_src/tutorial/005.py!}
```

# Transforms

Transforms are simply python functions with a decorator added to specify the
environment and the names of the inputs and outputs.

```python3
{!../docs_src/tutorial/006.py!}
```

It is important to note that because we added the decorator to the function
definition, the name parse_jv_csv no longer refers to a python function.
Instead, it refers to a DBgen Transform. The Generators section below shows how
DBgen Transforms are used in context.

# Loads

Loads are performed by calling the `load` classmethod on the `Entity` subclass
for the table we want to insert into. All of the identifying information for
that table must be supplied as keyword arguments when the `load` classmethod is
called. If we intend for the load to insert new rows (and not just update
existing rows), `insert=True` must also be passed to the `load` classmethod.
Just to show its simplicity, the line below shows a load statement to insert
rows into the JVCurve table, populating three columns: full path, open circuit
voltage, and short circuit current density.

```python3
{!../docs_src/tutorial/007.py!}
```
Taken out of context, this isn't very interesting, so, let's add some context to
this line by putting the extract, transform, and load all together into a
Generator.

# Generators

When a generator is defined, the actual code for the extracts, transforms, and
loads are not run. That happens later when the model is run. Defining a
generator simply stitches together the computational graph. In other words, we
are wiring the proper outputs from the extract to the proper inputs of the
transform and load, and we are wiring the proper outputs from the transform to
the proper inputs of the load.

```python3
{!../docs_src/tutorial/008.py!}
```



First, we created an instance of our custom LocalCSVExtract class and
passing in the data directory using an environment variable (this assumes that
the DATA_DIR environment variable is already set elsewhere to be a folder
containing jv curve csv's). We also assign the output of the e By creating this insance in the
with block for this generator, the extract is automatically added to this
generator.

Next, we assign the `file_path` key to a variable called `file_path`. The type
of this variable is a DBgen `Arg`, not a string. We are simply specifying where
the actual file path string will be sent when the model is rub. Again, in this
code, we are only wiring together the computational graph, not executing it.

Next, we see the transform definition copied directly from the Transforms
section above. After that, in the line `voc, jsc =
parse_jv_csv(file_path).results()`, by passing `file_path` (a DBgen Arg, which
knows that it is the output of the extract at key 'file_path') to parse_jv_csv,
we are specifying that, when the model is run, DBgen should go to the dictionary
yielded by the extract, look for key 'file_path', and pipe the string that it
finds there to the first input of the parse_jv_csv function. By calling
`.results()` on the transform, we get back a tuple of DBgen Args, which know
that they are the outputs of the parse_jv_csv function. Finally, in the load
statement, we again pass DBgen Args, this time to the load method of the table
we would like to insert into, specifying which outputs from the extract and
transform should be sent to which columns in the database.

# Models

A model is simply a set of generators. So, once the generators are defined, they
need to be added to the model. There are two ways to do that. The generators can
be added by defining them within a with block, as shown below.

```python3
{!../docs_src/tutorial/009.py!}
```

Alternatively, generators can be added using `model.add_gen(generator)`.

Whichever syntax is used, we must define a function that returns a model. This
function is what will be passed as a command line argument when the model is
actually run. Below is a complete example with pared down elements of everything
discussed above. This example creates a table called JVCurve with three columns
and populates them.

```python3
{!../docs_src/tutorial/010.py!}
```

Finally, to run the model, run `dbgen run --model path.to.model:make_model`