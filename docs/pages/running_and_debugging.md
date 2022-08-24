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

# Running and Debugging DBgen models

In this section, we walk through the most commonly used command line options that DBgen offers.

## dbgen connect

The `dbgen connect` command connects the user to the postgres database directly. It is the same as using the `psql ...` command and passing in the name of the database, the username, the password, and the port. Since dbgen needs all of that information to function anyway, dbgen exposes `dbgen connect` simply for convenience.

`dbgen connect --test` tests to make sure that the credentials that have been set in the .env file or passed to the command line are valid and allow DBgen to connect to the database. When setting up a new project or pointing an existing model at a new database, it is useful to run this command first to make sure that everything is configured correctly before you run the model.

## dbgen run

### Running Models

The `dbgen run` command is what is used to run the data pipeline. We'll walk through some of the commonly used options when using the `dbgen run` command. These options can be passed as command line arguments, or defaults can be set in the .env file, and global defaults exist for many of the options. DBgen will first use the values passed to the command line if they are present, then fall back on the values set in the .env file, and finally fall back on global defaults if they are not set in either the command line or the .env file.

### The --model option

To run dbgen models, we must define a function somewhere that returns the dbgen model. Then, when the model is run, the syntax is...

```bash
$ dbgen run --model [module_name]:[function_name]
```

...where the `[module_name]` and `[function_name]` refer to the location in the code where the function that returns the dbgen model is stored.

### The --build flag

When this flag is set, the existing database is torn down completely before the model is run. This needs to be done if you have made a change to the schema since the last time you ran the model.

### The --include and --exclude options

These options are used to only run specific ETLSteps. The syntax is `--include [regex]` or `--exclude [regex]`. In the `include` case, any ETLStep name that matches the regex will be run, and the others will not be run. Conversely, in the `exclude` case, any ETLSteps that match the regex will not be run, and all of the others will be run.

## Debugging Models

DBgen offers the following command line tools to assist with debugging.

### The --pdb flag

If you are actively developing a model and would like to insert a `breakpoint()` (also written `pdb.set_trace()`) in your code, you need to add the `--pdb` flag when running the model. This is because DBgen by default does a lot of exception handling to ensure that a bug in one ETLStep has as little impact as possible on the rest of the ETLSteps. The `--pdb` flag changes the exception handling protocol so that the python debugger (pdb) will work normally.
