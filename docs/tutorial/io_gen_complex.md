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

# Handling Data with Custom Formats

Oftentimes, scientific data is stored in custom file formats, and we need to be able to ingest this into the database. In this example, we assume there is a folder of temperature measurements on the local file system. The file names are of the format `FirstName_LastName_MeasurementNumber.txt`, and each file contains text similar to the following:

```
{!../docs_src/tutorials/alice-bob-model/data/measurements/Alice_Smith_0.txt!}
```

# Writing the Custom Extract

## Defining the Attributes

First, we subclass the Extract class (imported from dbgen) and define the necessary inputs.

```python3 hl_lines="9-12"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/extracts/measurement_extract.py [ln:1-12] !}
```

The `data_dir` is a string that specifies the name of the folder in which our text files are stored.

As always, we need to define the output names. In this case, since we need information from both the filename and the file contents, we will have two outputs.

Finally, we want to be able to read in the list of filenames in the `data_dir` in the `setup` method and then use that list in the `extract` method, so we will need a private attribute to store that list of filenames.

## Defining the setup method

Next, we need to overwrite the `setup` method. In this case, all we'll do is use `os.listdir` to get a list of the filenames in the folder that contains our text files.

```python3 hl_lines="14-15"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/extracts/measurement_extract.py [ln:1-15] !}
```

## Defining the extract method

Finally, we need to define the `extract` method. The extract method must always be a generator that yields dictionaries where the keys are the output names and the values are the corresponding output values. In this case, the two things that we are trying to output are two strings: the file name and the file contents.

We loop over the filenames stored in our private attribute, and simply read the file and return a dictionary containing the file name and file contents.

```python3 hl_lines="17-22"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/extracts/measurement_extract.py !}
```

Parsing these strings will happen in the transform step.


# Defining the corresponding transform

The first step to defining a transform is to define the output names and the python environment needed to run the function. In this case, we do not need any non-built-in python packages, so we can use our default python environment (`DEFAULT_ENV`).

We want our function to return four items: the researchers first name and last name, the order in which the measurement was taken, and the actual temperature measurement.

```python3 hl_lines="9-13"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/generators/parse_measurements.py [ln:1-20] !}
```

Next, we need to write a custom function that parses the filename and file contents to extract the information that we are interested in.


```python3 hl_lines="14-20"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/generators/parse_measurements.py [ln:1-20] !}
```

# Inserting the values into the database

Next, in order to insert these values into the database, we need to define a dbgen Generator. The standard pattern is to define a function that accepts the model and adds the new generator to the model. By using `with model`, all Generators defined in that with block will automatically be added to the model. Similarly, by using `with Generator(...)`, all extracts, transforms, and loads instantiated in that with block will automatically be added to the new Generator. Lines similar to the ones highlighted below are used almost every time a new Generator is defined.

```python3 hl_lines="1-3"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/generators/parse_measurements.py [ln:23-] !}
```

Next, we need to instantiate the custom extract we defined above. By calling `.results()` on the instance of our custom extract class, a tuple of the outputs is returned.

```python3 hl_lines="4"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/generators/parse_measurements.py [ln:23-] !}
```

After that, we want to pass the results from this extract to our custom transform (the parser defined above). Similarly, by calling `.results()` on the transform, a tuple of the outputs is returned.

```python3 hl_lines="5"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/generators/parse_measurements.py [ln:23-] !}
```

Finally, we call `.load(...)` on the table that we would like to insert data into, and we pass the values output by the transform as keyword arguments to the `.load` method. An important point is that any call to `.load` returns the ID of the row specified in the `.load(...)` statement. We do not always need to use this information, but we do need it to populate foreign keys. Simply put, foreign keys are always populated by calling the `.load` method on the table that you would like to create a foreign key to, as shown in the last line below.

```python3 hl_lines="6-11"
{!../docs_src/tutorials/alice-bob-model/alice_bob_model/generators/parse_measurements.py [ln:23-] !}
```
