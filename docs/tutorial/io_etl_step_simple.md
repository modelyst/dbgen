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

## Populating Tables

The three steps for populating tables are always: 1) extract, 2) transform, 3) load.

Let's walk through the code for populating the person table with data. Let's say
that the names of the researchers are currently stored in a .csv file that has
columns first_name, last_name, and age, which looks like this:

```
{!../docs_src/tutorial/names.csv!}
```

### Extract

First, we must write an Extract to get the data out of the file system. When defining extracts, we always subclass the Extract class, which is imported from DBgen.

Next, we define an attribute named "outputs" which always contains a list of strings that specify the names of the outputs. For example, in this CSV extract, we are just going to return one row of the csv at a time, so we have named the output "row."

Next, we can define any additional attributes that we want to supply when creating an instance of our Extract class later. In this case, the one attribute we will need is the location of the csv, which we have named `data_dir`.

Finally, if any internal attributes (not supplied by the user at the time of creating an instance) are needed for our extract class to function, we define those as `PrivateAttr`'s as shown below.

All that remains is to overwrite two methods: setup and extract. `setup` is always run before `extract`. In this case, all we do in the `setup` method is read the csv.

The `extract` method must be a generator that yields a dictionary where the keys are the names of the outputs. In this case, we have just one output named "row."

```python3
{!../docs_src/tutorial/tutorial002.py!}
```

### Transform

Next, we will walk through transforms and loads.

A `transform` is simply a function with an `@transform` decorator. In the decorator, we define the output names of the function and the python environment required to execute the function. We will walk through custom python environments in a later section of the tutorial.

```python3
{!../docs_src/tutorial/tutorial003.py!}
```

### Load

Finally, we need to create a DBgen "ETLStep" and add it to our model. The ETLStep contains the extract, transform, and load steps.

```python3
{!../docs_src/tutorial/tutorial004.py!}
```
