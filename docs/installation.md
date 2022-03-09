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


## Quick Start for Mac

Install PostgreSQL and DBgen.

```Console
$ brew install postgres
$ brew services start postgres
$ python -m venv .venv
$ source .venv/bin/activate
$ python -m pip install modelyst-dbgen
$ dbgen config -o .env
```

The last command creates a file called `.env` with some default settings. Edit the username and database name in the `.env` file.

```
# DBgen Settings
dbgen_main_dsn = postgresql://[username]@localhost:5432/[your_new_database_name]
dbgen_main_password =
```

By default, your `[username]` is the same as your Mac username, which can be found by entering the command `whoami`.

Create the new your new database.

```Console
$ psql -d postgres
# create database [your_new_database_name];
```

Enter the following command to confirm that setup is complete.

```Console
$ dbgen connect --test
```



## Prerequisites Explained

### SQL

DBgen requires a connection to a SQL database. The most basic configuration is to run PostgreSQL on the same machine that will run DBgen. Instructions for installing PosgreSQL are available <a href="https://www.postgresql.org/download/">here</a>. How to connect to local databases is covered in the "Basic Configuration" section below.

The database does not need to be running on the same machine as DBgen, and many dialects of SQL are supported (including SQLite, PostgreSQL, MySQL, Oracle, MS-SQL, Firebird, and Sybase). How to connect to remote databases is covered in the "Remote Configuration" section below.

### Python

DBgen requires python version 3.8.1 or newer. Instructions for installing python are available <a href="https://www.python.org/downloads/">here</a>.

### Required Python Packages

DBgen requires the python packages listed in <a href="https://github.com/modelyst/dbgen/blob/master/requirements.txt">requirements.txt</a> to be installed. This can be done easily by downloading the requirements.txt file and using pip to install them.

```Console
$ python -m pip install -r requirements.txt
```

## Installing DBgen

### Installing with pip

The easiest way to install dbgen is using pip. The name of the package on [pypi.com](https://pypi.org/project/modelyst-dbgen/) is `modelyst-dbgen`.

<div class="termy">
```Console
$ python -m pip install modelyst-dbgen
---> 100%
Successfully installed dbgen
```
</div>
Note that you can use the pip executable to install dbgen with the command `pip install modelyst-dbgen`, however, it is critical that the pip executable is pointing to the correct python or else dbgen can be installed to another python's site-packages. As it is common to isolate dbgen installations within their own virtual environments, the command `which pip` and `which python` should output paths within the same bin folder. If they don't then you can use the command shown above to force the use of the pip module within the selected python environment.
### Installing with Git

The easiest way to install dbgen is using pip. The name of the package on [pypi.com](https://pypi.org/project/modelyst-dbgen/) is `modelyst-dbgen`.

```Bash
{!../docs_src/installation/git_installation.sh!}
```

## Basic Configuration

The fastest way to get started is to navigate to the root directory for your project and run the command:

```Console
$ dbgen config -o .env
```

This will create a file named `.env`, which is where you can customize your own DBgen default settings. The only two crucial settings for getting started are the `dbgen_main_dsn` and the `dbgen_main_password`. These are needed for DBgen to connect to a database.

If you are running PostgreSQL on the same machine that you will use to run DBgen, the `dbgen_main_dsn` should be set to `postgresql://[username]@localhost:[port]/[database_name]` where `[username]` is your PostgreSQL username, the port is the port that PostgreSQL is running on (default is 5432), and the `[database_name]` is whatever you want to name your database.

```python
dbgen_main_dsn=postgresql://username@localhost:5432/my_database
```

The `dbgen_main_password` is your PostgreSQL password. This can be set in plain text in the .env file, although this method is not secure.

```python
dbgen_main_password=password_123
```

To increase security, it is recommended that you define an environmental variable in your .bashrc file that fetches the password from a secrets manager. Then, in your `.env` file:

```python
dbgen_main_password=$MY_PASSWORD
```

Finally, you'll need to create the database for DBgen to connect to. To do that, simply enter the command:

```Console
$ psql -d postgres
```

Then, enter:

```Postgres
postgres=## create database my_database;
postgres=## exit
```

To test that your setup is complete, enter the command:

```Console
$ dbgen connect --test
```

You should see green text indicating that the connection was successful.
