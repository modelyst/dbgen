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

# Directory Structure

Previously, in the "Getting Started" section, we showed than a valid DBgen pipeline can be written in 9 lines of code; however, in most real cases, a significantly more complex data pipeline is required.

Let's start by walking through the directory structure of a typical DBgen project.

You can follow along with this tutorial by copying some boiler plate code by running the command:
<div class='termy'>
```Console
$ dbgen new --template alice-bob-lab
<span style='color: green;'>Downloading template from https://github.com/modelyst/dbgen-model-template....</span>
```
</div>

This will prompt you to download the relevant files to a local directory with the directory structure shown below. You can find all the model templates at the <a href="https://github.com/modelyst/dbgen/tree/master/examples">DBgen Repo</a>.


```
├── data
├── alice_bob_model
    ├── extracts
    │   ├── __init__.py
    │   ├── extract_1.py
    ├── etl_steps
    │   ├── __init__.py
    │   ├── etl_step_1.py
    │   ├── etl_step_2.py
    ├── transforms
    │   ├── __init__.py
    │   ├── transform_1.py
    ├── __init__.py
    ├── constants.py
    ├── main.py
    ├── schema.py
```

## main.py

The overall goal of this entire repository is to create a DBgen model, which specifies the procedure to build the empty database and also to populate it with data. In `main.py`, there is a function that returns this DBgen model that the rest of the repository defines. This is a boiler-plate function, and a user does not need to ever change anything in this file.

The contents of `main.py` are shown below.


```python3 hl_lines="6-9"
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/main.py [ln:1-14] !}
```

## constants.py

This is a place to store any constants that are specific to the DBgen model defined in this repository. Common constants are:

- The path to a data source (if the data is stored locally)
- A cloud service (e.g. AWS) profile name and cloud storage location (if the data is stored remotely)
- The default python environment in which most of the functions will be run (more detail on defining python environments is later in this tutorial)

The contents of `constants.py` used in this tutorial are shown below.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/constants.py [ln:1-14] !}
```

## schema.py

This is the file that specifies the empty database schema. In other words, this is where we define the tables, columns (and their data type), and foreign keys in the database.

Part of the `schema.py` used in this tutorial is shown below.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/schema.py [ln:1-13] !}
```

## extracts

`Extracts` are relatively small pieces of code that define how to read data from a custom data source. We write that code here, then import it when we are writing ETLSteps later.

Below, we show an example of an `extract` that reads a csv stored in the local file system and returns its contents one row at a time. We'll walk through this in more detail later in the tutorial.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/extracts/csv_extract.py [ln:1-] !}
```

## transforms

In the transforms module, we write functions that parse or analyze incoming data. If the same function will be used multiple times, it is best to write the function in the transforms module and import it later when the ETLStep is written.

However, if the function is specific to a particular ETLStep and will not be reused, it is common to define that transform in the same file as the ETLStep (in the `etl_steps` module) instead of in the transforms module.

An example of a `transform` is shown below.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/read_csv.py [ln:10-16] !}
```

## etl_steps

The etl_steps module is where the code that populates the database is defined. Each ETLStep is one "Extract, Transform, Load" (ETL) step. The "extract" portion of the ETL step defines the source of the data. The "transform" defines which functions will be used to parse or analyze the data, and the "load" step defines where in the database the results will be stored.

The extract step may be a custom extract that we defined earlier, or it may be a query on the database.

The purpose of ETLSteps is essentially to define where data will come from, which function will analyze it, and where it will go.

Whenever we write a new ETLStep, we write a function that accepts the model as an input and adds that ETLStep to the model. An example is shown below.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/read_csv.py [ln:18-] !}
```

## etl_steps.\__init__.py

This is where we tell `main.py` to add the ETLSteps to the model. You can see that in `main.py`, we import a function called `add_etl_steps` from the `etl_steps.__init__.py` file. Then, `add_etl_steps` is the only function called within `make_model()`.

When we finish writing a new ETLStep, the last thing to is to add it to `etl_steps.__init__.py` so that `main.py` picks it up. The pattern is simple: for each ETLStep, import the function that adds that ETLStep to the model, and call that function in `add_etl_steps()` as shown below.

```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/__init__.py [ln:1-] !}
```
