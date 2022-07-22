## Unreleased

### Feat

- added model settings, etl tester and extractor api

### Fix

- **entity**: add init subclass to entity to remove type errors on table, and registry kwargs
- **cli**: add type checks to dbgen connect
- fix pre-release checkign in the release workflow

## v1.0.0a3 (2022-07-19)

### Fix

- **actions**: fix release publish action needs

## v1.0.0a2 (2022-07-19)

### Fix

- remove assertion in config test
- update psycopg to psycopg-binary
- add better error messages for invalid transform inputs
- update the config test
- minor bug fixes to async logging, poetry version and sql engine
- remove the cosigning from docker action

### Feat

- move non-remote runs to default and allow non-remote runs to not use env

## v1.0.0a1 (2022-06-21)

### Feat

- add docker-publish action
- add posix path dumper to psycopg
- **common extractors**: added a CSV extractor and an S3 extractor and relevant tests
- **docs**: added additional information to section 03 of the tutorial, fixed expandable code boxes, added acknowledgement to README
- **examples**: added the clean flag for the alice_bob_model
- **async**: initial commit of functional async code
- **error-messages**: improve error messages for loading into mangled databases
- **cli**: add fast-fail and fail-downstream CLI args as well as better error handling for failed runs
- **cli**: add more descriptive error messaging for models validation

### Fix

- remove publish docs github action on PR and only on publish on commit to master
- modify database syncing and copy_on_validation code
- creation of schemas for custom schemas
- add schema creation to test_model_sync
- remove copy from Base object and delete context before validation
- modify database syncing and copy_on_validation code
- check if list column is a list of lists before wrapping in singleton
- **dependencies**: fixed my mistake on pyyaml. Re-added pyyaml as explicit dependency
- **dependencies**: fixed my mistake regarding jinja2 dependency. It is now a dev dependency only.
- **poetry**: removed pyyaml as a dependency, keeping only types-pyyaml
- **examples**: fixed import location for extract in cookie cutter example
- **common extractors**: fix csv extractor length method and add reserved words to create_entity
- **common extractors**: move extractors to providers and add tests and error messages
- **docs**: added organization to docs and new error message for import failure
- **cli testing**: fix the testing errors due to main_dsn sharing
- **cli testing**: modify broken tests
- **cli testing**: remove broken tests
- **cli testing**: remove check of new dsn
- **cli testing**: pass the urls in through variables to avoid github sanitizing
- **cli testing**: fix the testing errors due to main_dsn sharing
- **typing**: fix the type hints to be compatible with python3.8
- **typing**: fix the typing for python 3.8
- **async**: added psycopg_pool to poetry requirements
- **docs**: added how to find psql username and download clean tutorial template
- **tests**: fixed the entity hypothesis strategy
- **errors**: fixed the error message for empty models
- small tuning of async
- **async**: fix the circular import in run

### Refactor

- **naming**: change the name of the nuke command to build
- **async**: major refactor of running to reduce code duplication for async/sync running
- **load**: generalized loading to reduce duplicate code for async/sync loading
- **tests**: remove unnecessary pytest fixtures and reenable the basic etl_step run

## v0.6.1 (2022-02-15)

### Feat

- **logging cli**: add --log-file cli option, cli docstrings, config error messaging

### Fix

- fix the links in README

## v0.6.0 (2022-02-14)

### Feat

- **config**: fix error in etl-step command
- **config**: added --chdir cli option, simplified global config
- **run**: Major overhaul of the running of models to align with new extract api and future async plans
- **config**: added batch_size to config file

### Fix

- remove Exception from raised sub-errors from transforms
- **cli**: fixed the links for dbgen new to point at dbgen

### Refactor

- **naming**: changed Generator->ETLStep
- **naming**: changed Pyblock-> PythonTransform
- **naming**: changed Env -> Environment and Const -> Constant
- **examples**: moved examples from template repo

## v0.5.19 (2022-02-09)

### Fix

- **ci**: update the poetry version to allow for publishing
- **docs**: removed broken links in docs

### Feat

- **docs**: moved alice bob model to sub repo for easy cookie cutter download
- **cli**: migrated styles to rich and added model validate

## v0.5.18 (2022-02-02)

## v0.5.17 (2022-02-02)

### Feat

- **deps**: move rich to package and remove python compatability 3.7

## v0.5.16 (2022-02-02)

### Feat

- **naming**: renamed loading id column to id from table_name
- **cli**: added the new command to CLI tool
- **logging**: replaced colorama/prettytable with rich
- **hello_world_docs**: added documentation page for a hello world DBgen model.
- **validation**: validate entity kwargs and change all_id to all_identifying
- **validation**: add required validation when insert=True

### Fix

- **query**: many statement parsing and batch query optimizations
- **query**: fixed the parametrized queries
- **dependency**: major fix to dependency tracking
- **validation**: modify validation to only raise required field errors on insert
- **tests**: removed old tests
- **query**: fix bind parameters for where clause

### Refactor

- **decorators**: removed the TypeArg class now that Arg is a generic class

## v0.5.11 (2021-11-10)
