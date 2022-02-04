from hashlib import new
import numpy as np
import csv

with open("names.csv", "r") as f:
    reader = csv.reader(f)
    for row in reader:
        fname = row[0]
        lname = row[1]
        for i in range(3):
            with open(f"measurements/{fname}_{lname}_{i}.txt", "w") as newfile:
                newfile.write(f"T (F): {np.random.randint(60, 80)}")
