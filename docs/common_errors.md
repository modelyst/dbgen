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

# Common Errors

In this section, we walk through the most common error messages that arise when working with DBgen and how to handle them.

## Data in the database don't match your expectations

### Only one row

#### Symptom

You see only one row where you expected to see many. The one value in the database is the last value that was processed. `dbgen run status` shows that the ETLstep in question processed the expected number of inputs but only made one insertion.

#### Possible Cause

If a ETLstep is only inserting one row where you expected it to insert many, you may have forgotten to set the `__identifying__` attribute on the table in question. An example that illustrates the problem is shown below.


```python3 hl_lines="5-7"
{!../docs_src/tutorials/errors/only_one_row.py [ln:1-] !}
```

In this example, `dbgen run status` will show that only one row was inserted even though 10 inputs were processed. Connecting to the database and querying the `Number` table will show only one row with the last number processed (9).

## Connection Errors

When connecting to a new database, it is generally useful to run `dbgen connect --test` first to determine whether or not dbgen is able to connect to the database. Below, we walk through the most common errors that occur when connecting to a new database and how to resolve them.

### Role postgres does not exist

#### Symptom

If you see `FATAL:  role "postgres" does not exist` in your error message, this means that dbgen is trying to connect to the database using the username `postgres`, which is not your postgres username.

```shell
(.venv) ➜  alice_bob_lab git:(documentation) ✗ dbgen connect --test
---------------------------------------------
Checking Main DB...
Trouble connecting to database at postgresql://postgres:******@localhost:5432/dbgen.
(psycopg2.OperationalError) connection to server at "localhost" (::1), port 5432 failed: FATAL:  role "postgres" does not exist

(Background on this error at: https://sqlalche.me/e/14/e3q8)
Cannot connect to Main DB at postgresql://postgres:******@localhost:5432/dbgen?options=--search_path%3dpublic!
---------------------------------------------
Checking Meta DB...
Trouble connecting to database at postgresql://postgres:******@localhost:5432/dbgen.
(psycopg2.OperationalError) connection to server at "localhost" (::1), port 5432 failed: FATAL:  role "postgres" does not exist

(Background on this error at: https://sqlalche.me/e/14/e3q8)
Cannot connect to Meta DB at postgresql://postgres:******@localhost:5432/dbgen?options=--search_path%3ddbgen_log!
```

#### Possible Causes

1) `postgres` is the default username for some postgres installations but not all. Another common default username is the user's bash username. To see what your bash username is, enter the command `whoami` into the command line. If this is the problem, updating your DBgen `.env` file with that username
will solve it.

In `.env`, make sure this line has the correct username:

```dbgen_main_dsn = postgresql://[your_username]@[host]:[port]/[database_name]```

2) Your `.env` could not be found

If you have set the username to a value other than `postgres` in a DBgen `.env` file, and you are still getting this error message, then DBgen is not finding your .env file and is falling back on global defaults. DBgen looks for .env files in the following places (in order):

1. The location passed after the `-c` flag
2. The current directory
3. The location set in the environmental variable `$DBGEN_CONFIG`

If DBgen was recently able to connect to the database but stopped being able to connect after you changed directories, it is likely that it had found a `.env` file in the working directory before you changed directories but not after.

## Errors when running models

### Missing imports in the transform

#### Symptom

You are running a ETLstep that includes a transform that requires an import statement to run, and when you call `dbgen run ...`, you see an error that includes something that looks like:

```shell
NameError: name 're' is not defined
Error when running ETLstep ints
Error encountered while applying function named 'parse_string'
```

Below is an example that produces this error:

```python3 hl_lines="23-30"
{!../docs_src/tutorials/errors/missing_imports.py [ln:1-] !}
```

#### Possible Cause

Although the package that your transform depends on may well already be imported at the top of the file where the transform is defined, you must also make sure that the environment passed to the decorator `@transform(env=my_env)` includes the package or function that your transform depends on. To do that, add an `Env` to the existing one and include the missing package.

```python3
new_env = my_env + Env([Import("new_package")])
@transform(env=new_env, outputs=...)
def some_transform(...):
    ...
```

### Mismatch between the number of output names and the number of outputs returned by a transform

#### Symptom:

You are running a model and one of the ETLSteps produces an error that looks this:

```TypeError: You are attempting to iterate/unpack the arg object Arg(c58d...,wrong). This can commonly occur when a Extract or Transform outputs a single output when you expected two. Did you remember to set the outputs on your Extract or Transform?```

A model that produces this type of error is shown below.

```python3 hl_lines="21-24"
{!../docs_src/tutorials/errors/transform_output_count_mismatch.py [ln:1-] !}
```
#### Possible Cause

You will get an error like this if the number of the number of variables returned by a transform is not the same as the length of the list of output names (defined in `@transform(outputs=[...])`).

You will also see this error if you do not set a value for `outputs` at all in your `@transform` definition, and your function returns multiple outputs. If you do not specify any output names, then DBgen assumes that the transform has only one output and gives it the name `"out"`.

In either case, the solution to this problem is to define a list of output names in `@transfom(outputs=[...])` that is the same length as the number of outputs that your function returns.

### Missing Identifying Information

#### Symptom

You are running a model and one of the ETLSteps produces an error that looks this:

```
DBgenMissingInfo: Cannot refer to a row in number without a PK or essential data. Missing essential data: {'integer'}
 Did you provide the primary_key to the wrong column name? the correct column name is the table name (i.e. number=...)
```

A model that produces this type of error is shown below.

```python3 hl_lines="21-24"
{!../docs_src/tutorials/errors/missing_id_info.py [ln:1-] !}
```

#### Possible Cause

Whenever you see this error, it means that you are trying to insert new rows or update existing rows, but you have not supplied the identifying information for the row you are trying to update or insert. Go to the `.load()` statement that is producing the error, and look at the `Entity` (meaning the Entity referred to in `EntityName.load(...)`). Then, go to the definition of that `Entity` and look at the set of column names that appear in `__identifying__`. This is the information that you need to supply in your load statement.

In the case of inserting new rows, in your `.load(insert=True, ...)` statement, you must supply values for each of the pieces of identifying information as keyword arguments:

```
EntityName.load(
    insert=True,
    column_name_from_Entity_definition = variable_name_from_transform_or_extract,
    ...
)
```

In the case of updating existing rows, you have the option of either supplying the identifying information for the row (as shown above) or the ID string for the row, which will always come from a query (as shown below).

```
id_string, raw_value = Query(select(EntityName.id, EntityName.raw_value)).results()
refined_value = a_transform(raw_value).results()
EntityName.load(
    id=id_string,
    refined_value=refined_value
)
```

## Installation Errors

### dbgen executable can not be found
#### Symptom
<div class="termy">
```Console
$ dbgen version
command not found: dbgen
```
</div>
#### Possible Cause
This error occurs when the dbgen executable cannot be found in the $PATH variable of your shell. This is commonly caused by two main issues:

1. You have not correctly sourced the virtual environment in which dbgen was installed.

Solution: activate the virtual environment

2. DBgen has not been installed through pip or poetry

Solution: Install dbgen through the pip or poetry methods shown in the <a href="/installation">installation</a> section
### Missing module error
#### Symptom
<div class="termy">
```Console
$ dbgen version
ModuleNotFoundError: No module named 'rich'
```
</div>
#### Possible Cause
This error occurs when a dependency of dbgen has not been installed. This can occur if the executable was installed to an incorrect python environment. This occurs when the pip executable from one python installation is used to install dbgen. You can be sure this is the issue if the command `python -m dbgen version` does not cause issues, as this forces the use of the specific python installation first in your $PATH.

Uninstalling dbgen with the command `pip uninstall modelyst-dbgen` and then reinstalling it in the correct location with `python -m pip install modelyst-dbgen` will usually solve this issue. Just make sure to activate the virtual environment prior to running the installation command.
