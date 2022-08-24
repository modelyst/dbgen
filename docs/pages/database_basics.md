<!--
   Copyright 2022 Modelyst LLC

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

# Database Basics

In this section, we walk through the basic concepts behind databases and the terms used to describe them.

## What is a database?

A database is a simply a set of database "tables."

## What is a database table?

A database table looks a lot like a simple spreadsheet. Each table has one or many columns, The table can then be filled with many rows of data.

Database tables are much more strictly structured than spreadsheets.

In a spreadsheet, nothing stops a user from naming a column "date of birth" then writing "2" in the first row and "banana" in the second row. However, in a database, each column must have a name and a data type (like integer, string, floating point number, timestamp etc), and there is a technical guarantee that each value in that column is of that data type.

In a spreadsheet, a user does not need to create column headers at all. It is possible to start entering any data into any "cell" immediately. In a database table, those cells do not exist at the outset. A user must first define the column names and data types, at which point the table exists with column headers but has no rows. In the context of databases, a phrase like "put the number 3 in the 10th row of a given column of an empty table" is meaningless. The user can instead say, "add a row to a given table with the number 3 in a given column."

## What is a primary key?

A primary key is a unique identifier for a row in a database table. In almost all cases, one column is designated as the primary key. No two rows can have the same primary key, and the primary key for a given row can never be null.

## What is a many-to-one relation, and what are foreign keys?

So far we've discussed some properties of a single database table, but most databases have many tables that are related to each other. Foreign keys define these relationships between tables.

Let's start with an example. Say you have a table called "person" with columns like first name, last_name, and the city they are living in. One table does a perfectly good job of storing this information.

Now, however, let's say you want to start adding many properties of the cities to the database, like the population, the land area, the date it was established, etc. You could add these as columns to the person table, but you'll have to repeat the same values over and over again (e.g. the population of New York City will have to be added to the row for every person living in New York City). This is both wasteful from a storage space point of view and difficult to change because a large number of rows need to be changed when the population of New York is updated.

What we really want to define in this case is a table of people, a table of cities, and a many-to-one relationship between them (MANY people can live in one city, and a person can only currently live in ONE city). To do this, we define a separate table called city and give it a primary key column alongside any other columns we want to track (e.g. population, etc.). Finally, we add a "foreign key" column to the person table (usually called city_id) which references the primary key column of the city table.

<!-- So, a row in the person table will look like: -->

<!-- | first_name | last_name | city_id |
|----------------------------------|
|   Joe      |  Smith    |    1    |


| first_name | last_name | city_id |
|----------------------------------|
|   Joe      |  Smith    |    1    | -->


In technical terms, a foreign key is a column whose values are always the primary key of another table. Each foreign key creates a "many-to-one" relation between two tables.

## When should I use a database instead of spreadsheets and csv's?


- Data Complexity

If your data has only one-to-one relationships in it, a simple spreadsheet may work. However, if your data has many-to-one or many-to-many relations in it, it becomes challenging to use spreadsheets to accomplish this.

- Analysis Complexity

Especially when dealing with scientific data, we often want to carry out analyze or data in complex, customized ways, which is not always possible to do using simple spreadsheet operations (e.g. peak-finding). Additionally, even if the analysis is possible to carry out using a spreadsheet, spreadsheet formulas become difficult to read as soon as the complexity of the analysis goes beyond one or two calls to the built-in spreadsheet functions.

- Data Volume

Most spreadsheets can only store 10,000 - 100,000 rows of data before running into performance issues. However, even a database running on an average-powered computer can handle hundreds of millions of rows of data without performance problems.

## Signs that your use case has outgrown your spreadsheet system
- You see a lot of repeated values in your spreadsheets.

This is a sign that your data has many-to-one relations in it that are being handled by repeating the common values that apply to many rows.

<!-- - You are using a lot of pivot tables or "vlookup" function calls.

Vlookup functions and pivot tables are  -->

- You have many variants of the same csv.

This is a sign that you are interested in different subsets of your data at different times. Rather than saving new CSVs, you could query a single database for the exact subset of the data you need at any given time.

- Your spreadsheet formulas are hard to understand at a glance.

This is a sign that the complexity of your data analysis has outgrown what spreadsheets were meant to support.


<!-- ## How do the terms foreign key, relation, table, and entity relate to each other?

## What is "ETL"?

## What is a query? -->
