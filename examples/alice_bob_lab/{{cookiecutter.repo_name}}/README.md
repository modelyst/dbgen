# Alice and Bob Model



## Getting Started
Setup a valid dbgen virtual environment and install the requirements of this model

```Console
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration
Make sure to set the relevant database credentials so that dbgen can connect to the database you want to access.

The two most important variables are DBGEN_MAIN_DSN and DBGEN_MAIN_PASSWORD. If you set the DSN during the cookiecutter setup it should automatically be picked up by dbgen  and stored in the .env file. You can confirm this by running `dbgen config` to see the current configuration.

The password to the database is usually set using an environmental variable to avoid storing passwords in plain text. Some local installations of postgresql do not require passwords for users. If this is the case, you may leave this env variable null.

To test the connection, please run the command `dbgen connect --test`. If successfully configured, a green success message should  be printed to the screen.
