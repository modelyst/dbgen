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

## Full Files

The code snippets in this section are taken from `extracts/csv_extract.py` and `etl_steps/read_csv.py`. The full files are shown below:

<details>
<summary>alice_bob_model/extracts/csv_extract.py</summary>

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/extracts/csv_extract.py [ln:1-] !}
```
</details>

<details>
<summary>alice_bob_model/etl_steps/read_csv.py</summary>

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/read_csv.py [ln:1-] !}
```
</details>

## Populating Tables

The three steps for populating tables are always: 1) extract, 2) transform, 3) load.

Let's walk through the code for populating the person table with data. Let's say
that the names of the researchers are currently stored in a .csv file that has
columns first_name, last_name, and age, which looks like this:

```
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/data/names.csv!}
```

### Extract

First, we must write an Extract to get the data out of the file system. When defining extracts, we always subclass the Extract class, which is imported from DBgen.

Next, we define an attribute named "outputs" which always contains a list of strings that specify the names of the outputs. For example, in this CSV extract, we are just going to return one row of the csv at a time, so we have named the output "row."

Next, we can define any additional attributes that we want to supply when creating an instance of our Extract class later. In this case, the one attribute we will need is the location of the csv, which we have named `data_dir`.

Finally, if any internal attributes (not supplied by the user at the time of creating an instance) were needed, we would define those below the user-supplied attributes, and they would need to begin with an `_` character.

All that remains is to overwrite the `extract` method. This method must be a generator that yields either:

1. a tuple the same length as the list of output names. If this syntax is used, DBgen will assume that the output names and the variables in the returned tuple are in the same order.
2. a dictionary where the keys are the same as the output names specified earlier, and the values are the corresponding values.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/extracts/csv_extract.py [ln:1-] !}
```

### Transform

Next, we will walk through transforms and loads.

A `transform` is simply a function with an `@transform` decorator. In the decorator, we define the output names of the function and the python environment required to execute the function.

In this case, the transform function is going to accept one row (a list of strings) from the `extract`. It is simply going to return the first name (a string), last name (a string), and age (an integer).

In the `@transform` decorator, we set the `outputs` and `env`. The `outputs` is always a list of strings of the same length as the number of outputs that the function returns. This assigns names to the outputs that can be referred to later if desired. The order of the names given in the `outputs` list matches the order of the variables returned by the function.

In this case, the transform function does not require any imported python packages, so we will set the `env` to the `DEFAULT_ENV`, which we import from `constants.py`. Later in this tutorial, there will be an example of a transform function that does require an import statement, and we will walk through custom python environments in that section.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/read_csv.py [ln:2,4,8-19] !}
```

### Load

Finally, we need to create a DBgen "ETLStep" and add it to our model. The ETLStep contains the extract, transform, and load steps.

When defining any `ETLStep`, all we are really doing is specifying where data is coming from, which transformations it is being passed through, and which columns in the database it should end up in. In other words, we are wiring the outputs from data sources (`extracts`) to the inputs of data transformations (`transforms`), and the outputs of data transformations to the inputs of `load` statements, which specify the final location of data in the database. In the script below, we are not actually making any calls to the extract, transform or load; we are not actually handling any of the data that will eventually flow through this data pipeline, and we are not making any insertions into the database. Instead, we are defining the build procedure for the data pipeline. To take the analogy one step further, we are connecting all the pipes, but we do not turn the water on until we call `dbgen run`, which is discussed in the next section.

In the script below, the lines of code corresponding to the extract, transform, and load are labeled with in-line comments.

In the `extract` line, we create an instance of the custom `extract` that we had previously defined. Recall that this extract yields just one output: a row in the csv. By calling `.results()` on the instance of our custom extract, a tuple of the same length as the number of outputs is returned. In this case, there is only one output, so the single output is assigned to the variable `row`. It is important to note that the variable `row` in this script does not contain a row of data from the CSV file; rather, it is a DBgen `Arg` object, which you can think of as a named pipe in the data pipeline. So, in this line of code, we are specifying that there is a pipe called `row` coming out of this extract, and in subsequent lines of code, we will specify where that pipe connects to.

In the `transform` line, we pass the `row` from the previous line of code as the only input to the transform that we defined previously in this section of the tutorial. Again, by calling `.results()` on the transform, a tuple of the same length as the number of outputs is returned. In this case, the transform has three outputs, which we assign to variables named `first_name`, `last_name`, and `age`. Similarly to the previous line of code, these variables do not contain names or ages from the actual data source. Instead, they are objects that are used to specify which inputs are connected to which outputs. So, in this line of code, we have specified that the pipe named `row` from the output of the extract is connected to the sole input of the transform called `parse_names`, and this transform has three pipes coming out of it. We will specify where those pipes connect to in the next line.

In the `load` line, we specify which outputs from previous extracts and transforms should be inserted into which rows in the database. To do this, we always call the `.load()`class method on the table in which we would like to insert or update rows. The class representing table is imported from `schema.py`. Whenever we would like to insert rows (as opposed to update existing rows), we must set `insert=True`. After that, we include keyword arguments where the keywords are column names (as specified in `schema.py`), and the arguments are names of DBgen `Arg` variables that have come from previous extracts and transforms. Recall that every table has a set of identifying columns (set in `__identifying__ = {...}` in the definition of the table in `schema.py`). Whenever we are inserting new rows into the database, we must at least supply keyword arguments for the identifying columns, and we may optionally keyword arguments for any additional columns. In this case, we supply keyword arguments for both the identifying columns ("first_name" and "last_name") as well as the additional column, "age." In summary, the keywords in `.load()` statements must match column names from the Entity definition in `schema.py`, and the values of the arguments must match variable names defined previously in the script. In this case, we have specified that the three pipes coming out of the transform (`first_name`, `last_name`, and `age`) are connected to three columns in the person table, which happen to have the same names.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/read_csv.py [ln:1-] !}
```

### Running the Model

Now that the model has one complete ETL step, we can run the model, which creates and populates the database. The command `dbgen run` is used to run models. Since this is the first time we are running the model, we need to add the `--build` flag.

```dbgen run --build```

To see information about the attempted run of the model, enter the command `dbgen run status`. In this case, we should see that 10 rows have been inserted.
