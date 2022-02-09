# DBgen Overview and Concepts

Below, we introduce the most important concepts in DBgen.

## Models

A `Model` is the conceptually "largest" object in DBgen. A `Model` contains all of the information required to build a database. When defining a `Model`, there are two key steps:
- Define the tables and columns that will exist in your database.
- Define the procedure for populating those tables with actual data.

In order to accomplish the first step, we need to understand DBgen "Entities," and in order to accomplish the second step, we need to understand DBgen "Extracts", Transforms", Loads", and "Generators". These concepts are introduced below.

## Defining the empty database schema

### Entities

An `Entity` in DBgen defines a database table. When we define a new `Entity`, we define the name of the table, and for each column, the name of the column and its data type. Some of the columns may be foreign keys that refer to previously-defined `Entities`. Creating a foreign key from one table to another specifies a many-to-one relation between those two tables.

This set of tables, attributes, and relationships is known as the database "schema."

The name `Entity` refers to the <a href="https://en.wikipedia.org/wiki/Entity%E2%80%93relationship_model">"entity-relationship" model</a> for describing the properties of and relationships between the objects of interest in a given scenario.

## Populating the database with data

There are many tools available that assist with creating empty database schemas. A large part of what makes DBgen unique is that it provides a framework for populating databases with data.

The process of extracting data from one location, transforming it by passing it through one or more functions, and loading it into a database is referred to as the "Extract, Transform, Load" (or ETL) process.

### Generators

The ETL process is often comprised of many ETL steps. In DBgen, each ETL step is a `Generator`, and each `Generator` has an Extract, a Transform, and a Load.

### Extracts

In DBgen, `Extract`s can be either:

- Queries against the database
- Custom-defined pieces of code that read data from a specified location like a local file system or a remote cloud storage system.

### Transforms

In DBgen, a `transform` is simply a python function with a decorator above it, which specifies the names of the outputs of the python function and any non-built-in imports that are required for the function to run.

### Loads

In DBgen, every `Entity` has a `.load()` method built in, and that is used to specify the destination in the database (which table and column) where the results of the extracts or transforms belong.

## Summary

The goal is to define a database build procedure. The steps to do that are:

- Define the empty schema
    - Define the tables, columns, and foreign keys
- Define the data population procedure
    - IO generators
        1. Define extracts to read data from its original location
        2. Define transforms to parse raw data
        3. Load this raw data into the database
    - Analysis generators
        4. Query for raw data
        5. Define transforms that analyze raw data
        6. Load the results of analyses into the database
- Add the generators to the model, and run.

The outline above describes a model with a single IO generator and and a single analysis generator. A real DBgen model will generally consist of at least one IO generator and many analysis generators. It is worth noting that there is no technical distinction in DBgen between an "IO Generator" and an "Analysis Generator;" these terms are only meant to describe the purpose of the generator.