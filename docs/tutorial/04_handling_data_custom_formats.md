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

<details>
<summary>Excerpt from: alice_bob_model/data/measurements/Alice_Smith_0.txt</summary>
```
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/data/measurements/Alice_Smith_0.txt!}
```
</details>


# Writing the Custom Extract

## Defining the Attributes

First, we subclass the Extract class (imported from dbgen) and define the necessary inputs.

<details>
<summary>Excerpt from: alice_bob_model/extracts/measurement_extract.py</summary>
```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/extracts/measurement_extract.py [ln:1-26] !}
```
</details>

The `data_dir` is a string that specifies the name of the folder in which our text files are stored.

As always, we need to define the output names. In this case, since we need information from both the filename and the file contents, we will have two outputs.

## Defining the extract method

Finally, we need to overwrite the `extract` method. As always, the extract method must be a generator. In this case, we would like to output two strings: the file name and the file contents.

We get a list of the filenames in the `data_dir` then loop over the filenames and simply read the file and yield the name of the file and the contents of the file.

<details>
<summary>Excerpt from: alice_bob_model/extracts/measurement_extract.py</summary>
```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/extracts/measurement_extract.py !}
```
</details>

These strings will be parsed in the transform step. In general, if there is something that can be done in either the extract step or the transform step, it is better to do it in the transform step because the exception handling for transforms is more advanced. Extracts should remain as simple as possible.


# Defining the corresponding transform

The first step to defining a transform is to define the output names and the python environment needed to run the function. In this case, we do not need any non-built-in python packages, so we can use our default python environment (`DEFAULT_ENV`).

We want our function to return four items: the researchers first name and last name, the order in which the measurement was taken, and the actual temperature measurement.

<details>
<summary>Excerpt from: alice_bob_model/etl_steps/parse_measurements.py</summary>
```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/parse_measurements.py [ln:1-10] !}
```
</details>

Next, we need to write a custom function that parses the filename and file contents to extract the information that we are interested in.

<details>
<summary>Excerpt from: alice_bob_model/etl_steps/parse_measurements.py</summary>
```python3
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/parse_measurements.py [ln:1-32] !}
```
</details>

# Inserting the values into the database

Next, in order to insert these values into the database, we need to define a dbgen ETLStep. The standard pattern is to define a function that accepts the model and adds the new ETLStep to the model. By using `with model`, all ETLSteps defined in that with block will automatically be added to the model. Similarly, by using `with ETLStep(...)`, all extracts, transforms, and loads instantiated in that with block will automatically be added to the new ETLStep. Lines similar to the ones highlighted below are used almost every time a new ETLStep is defined.

<details>
<summary>Excerpt from: alice_bob_model/etl_steps/parse_measurements.py</summary>
```python3  hl_lines="2-3"
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/parse_measurements.py [ln:33-] !}
```
</details>

Next, we need to instantiate the custom extract we defined above. By calling `.results()` on the instance of our custom extract class, a tuple of the outputs is returned.

<details>
<summary>Excerpt from: alice_bob_model/etl_steps/parse_measurements.py</summary>
```python3 hl_lines="4"
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/parse_measurements.py [ln:33-] !}
```
</details>

After that, we want to pass the results from this extract to our custom transform (the parser defined above). Similarly, by calling `.results()` on the transform, a tuple of the outputs is returned.

<details>
<summary>Excerpt from: alice_bob_model/etl_steps/parse_measurements.py</summary>
```python3 hl_lines="5"
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/parse_measurements.py [ln:33-] !}
```
</details>

Finally, we call `.load(...)` on the table that we would like to insert data into, and we pass the values output by the transform as keyword arguments to the `.load` method. An important point is that any call to `.load` returns the ID of the row specified in the `.load(...)` statement. We do not always need to use this information, but we do need it to populate foreign keys. Simply put, foreign keys are always populated by calling the `.load` method on the table that you would like to create a foreign key to, as shown in the last line below.

<details>
<summary>Excerpt from: alice_bob_model/etl_steps/parse_measurements.py</summary>
```python3 hl_lines="6-11"
{!../examples/alice_bob_lab/{{cookiecutter.repo_name}}/alice_bob_model/etl_steps/parse_measurements.py [ln:33-] !}
```
</details>


### Running the Model

We can run the model again to see the effects of our new ETL step. To run the model, enter the command:

```dbgen run```

To see information about the attempted run of the model, enter the command `dbgen run status`. In this case, we should see that 30 rows have been inserted.
