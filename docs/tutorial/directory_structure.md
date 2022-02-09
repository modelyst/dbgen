# Directory Structure

Previously, in the "Getting Started" section, we showed than a valid DBgen pipeline can be written in 9 lines of code; however, in most real cases, a more complex data pipeline is required.

Let's start by walking through the directory structure of a typical DBgen project. You can get started quickly with a directory structure like this with some boiler plate code by cloning the <a href="https://github.com/modelyst/dbgen-model-template">DBgen cookie cutter repo</a>.

<!-- ![](../docs_src/directory_screenshot.png)

<img src="docs_src/directory_screenshot.png"/> -->

```
├── data
├── extracts
│   ├── __init__.py
│   ├── extract_1.py
├── generators
│   ├── __init__.py
│   ├── generator_1.py
│   ├── generator_2.py
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


```python3 hl_lines="9-12"
{!../docs_src/tutorials/alice_bob_lab/main.py [ln:1-12] !}
```

## constants.py

This is a place to store any constants that are specific to the DBgen model defined in this repository. Common constants are:

- The path to a data source (if the data is stored locally)
- A cloud service (e.g. AWS) profile name and cloud storage location (if the data is stored remotely)
- The default python environment in which most of the functions will be run (more detail on defining python environments is later in this tutorial)

The contents of `constants.py` used in this tutorial are shown below.

```python3 hl_lines="9-12"
{!../docs_src/tutorials/alice_bob_lab/constants.py [ln:1-12] !}
```

## schema.py

This is the file that specifies the empty database schema. In other words, this is where we define the tables, columns (and their data type), and foreign keys in the database.

Part of the `schema.py` used in this tutorial is shown below.

```python3
{!../docs_src/tutorials/alice_bob_lab/schema.py [ln:3-12] !}
```

## extracts

`Extracts` are relatively small pieces of code that define how to read data from a custom data source. We write that code here, then import it when we are writing generators later.

Below, we show an example of an `extract` that reads a csv stored in the local file system and returns its contents one row at a time. We'll walk through this in more detail later in the tutorial.

```python3
{!../docs_src/tutorials/alice_bob_lab/extracts/csv_extract.py [ln:1-] !}
```

## transforms

In the transforms module, we write functions that parse or analyze incoming data. If the same function will be used multiple times, it is best to write the function in the transforms module and import it later when the generator is written.

However, if the function is specific to a particular generator and will not be reused, it is common to define that transform in the same file as the generator (in the `generators` module) instead of in the transforms module.

An example of a `transform` is shown below.

```python3
{!../docs_src/tutorials/alice_bob_lab/generators/read_csv.py [ln:10-16] !}
```

## generators

The generators module is where the code that populates the database is defined. Each generator is one "Extract, Transform, Load" (ETL) step. The "extract" portion of the ETL step defines the source of the data. The "transform" defines which functions will be used to parse or analyze the data, and the "load" step defines where in the database the results will be stored.

The extract step may be a custom extract that we defined earlier, or it may be a query on the database.

The purpose of generators is essentially to define where data will come from, which function will analyze it, and where it will go.

Whenever we write a new generator, we write a function that accepts the model as an input and adds that generator to the model. An example is shown below.

```python3
{!../docs_src/tutorials/alice_bob_lab/generators/read_csv.py [ln:18-] !}
```

## generators.\__init__.py

This is where we tell `main.py` to add the generators to the model. You can see that in `main.py`, we import a function called `add_generators` from the `generators.__init__.py` file. Then, `add_generators` is the only function called within `make_model()`.

When we finish writing a new generator, the last thing to is to add it to `generators.__init__.py` so that `main.py` picks it up. The pattern is simple: for each generator, import the function that adds that generator to the model, and call that function in `add_generators()` as shown below.

```python3
{!../docs_src/tutorials/alice_bob_lab/generators/__init__.py [ln:1-] !}
```


