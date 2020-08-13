# Internal Modules
from dbgen import Model, Generator, SUM, Query

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
