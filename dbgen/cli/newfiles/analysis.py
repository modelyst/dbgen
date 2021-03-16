#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Internal Modules
from dbgen import SUM, Generator, Model, Query

################################################################################


def analysis(mod: Model) -> None:

    # Extract tables
    tabs = ["employee", "department"]

    # Extract Step
    Emp, Dept = map(mod.get, tabs)
    emp_path = mod.make_path("employee", [Emp.r("department")])
    exprs = {"tot_salary": SUM(Emp["salary"](emp_path)), "emp_id": Dept.id()}
    query = Query(exprs=exprs, aggcols=[Dept.id()], basis=["department"])
    # Load Step
    dept_load = Dept(department=query["emp_id"], total_salary=query["tot_salary"])

    get_tot_salary = Generator(
        name="get_total_salary",
        desc="Salary",
        transforms=[],
        tags=["analysis"],
        loads=[dept_load],
        query=query,
    )

    ################################################################################
    gens = [get_tot_salary]
    mod.add(gens)
