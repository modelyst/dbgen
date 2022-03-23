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

# Hello World DBgen Model

This page walks through the simplest possible DBgen model. We'll start by showing the complete code then describing what each part does.

Create a file called `hello_world.py` that contains this code:

```python3
{!../docs_src/hello_world/hello_world.py!}
```

Then, to run the model (and therefore build the database and populate it), run from the command line:

```bash
$ dbgen run --model hello_world:make_model --build
```

To see the result, you can run the following command to connect to the database...

```bash
$ dbgen connect
```
... then run the following command to see what is in the table:

```bash
select * from SimpleTable;
```

You should see a table with one column called `string_column` with one row containing `hello world`.


## Step 1: Make an Empty Database

In the above example (and in any DBgen model), the first step is to create an empty database. In this step, the database tables and their attributes are defined.

In this example, there is only one table called "SimpleTable," and it has only one column called "string_column." Whenever you want to make a new database table using DBgen, you create a new class that subclasses the DBgen "Entity" class with `table=True` as shown in this example. Then, to add a column, we always write `[column_name]: [data_type]`.


## Step 2: Fill the Database

In the above example (and in any DBgen model), the second step is to define the procedure to fill the database with data. Every step in the procedure that fills the database is called an "ETLStep" in DBgen.

In this example, the model has only one ETLStep called "simplest_possible." The best way to define ETLSteps and add them to the DBgen model is to use with blocks, as shown above.

ETLSteps are capable of extracting data from a variety of sources, transforming it using any arbitrary python function, and loading the results into the database. In this example, our simplest_possible ETLStep does not have any extracting or transforming code. It simply loads a constant value into the column called "string_column."

To load data into a table, we always write `[TableName].load(insert=True, ...)` then `[column_name]: [value_to_be_inserted],` as keyword arguments for each value that we want to insert.

## Step 3: Running the Model

To run the model, the command is always `python -m dbgen run` with the `-- model` option set to a function that returns a dbgen model. That function is specified by typing the python module name, then a colon, then the function name (`[big_project.smaller_module]:[name_of_function_that_returns_model]`)

## Next Steps

If words like schema, table, attribute, foreign key, ETL, and query are unfamiliar to you and you want to read more high-level information about databases and how you can use DBgen to create them, please visit the "DBgen Overview and Concepts" page.

DBgen is designed to handle very complex database schemas and build procedures. If you want to learn more about defining more DBgen models, please visit the "Tutorial" page.

The in-detail technical documentation for the DBgen python objects, attributes, and methods are available in the Technical Documentation section.
