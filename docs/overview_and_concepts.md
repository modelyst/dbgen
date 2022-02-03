# DBgen Overview and Concepts

Below, we introduce the most important concepts in DBgen.

## Models

A Model is the conceptually "largest" object in DBgen. A Model contains all of the information required to build a database. When defining a Model, there are two key steps:
- Define the tables and columns that will exist in your database.
- Define the procedure for populating those tables with actual data.

In order to accomplish the first step, we need to understand DBgen "Entities," and in order to accomplish the second step, we need to understand DBgen "Extracts", Transforms", Loads", and "Generators". These concepts are introduced below.

## Entities

An Entity in DBgen is 