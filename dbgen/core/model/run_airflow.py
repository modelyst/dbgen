# External imports
from typing  import TYPE_CHECKING, Any

import shutil
from glob import glob
from os      import environ
from os.path import join, abspath, dirname, exists
from jinja2  import Template
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook # type: ignore
# Internal Imports
if TYPE_CHECKING:
    from dbgen.core.model.model import Model
from dbgen.core.misc import ConnectInfo
##################################
def run_airflow(self      : 'Model',
                sched     : str = '@once',
                nuke      : str = '',
                **kwargs  : Any
                ) -> None:
    '''
    Create an airflow DAG, then execute it.
    '''
    # ping the database and check if we need to add Objs and Relations
    connection  = PostgresHook.get_connection(self.name)
    connI = ConnectInfo.from_postgres_hook(connection)
    mconnection = PostgresHook.get_connection(self.name+'_log')
    mconnI = ConnectInfo.from_postgres_hook(mconnection)


    if nuke:
        self.make_schema(conn=connI,nuke=nuke) # FULL NUKE

    # Check if the schema exists
    if not self.check_schema_exists(connI):
        raise ValueError('Your Schema doesn\'t exist yet, please run with --nuke=T the first time')

    # Make metatables
    #----------------
    run_id = self._make_metatables(mconn=mconnI,conn=connI,nuke=nuke,retry=False,only='',xclude='',start='',until='',bar=False)


    operators     = {gn:g.test_operator(self.name, self.objs).replace('\n','\n    ') for gn,g in self.gens.items()}
    deps          = list(self._gen_graph().edges())
    template_dict = dict(user              = environ['USER'],
                         modelname         = self.name,
                         operators         = operators,
                         deps              = deps,
                         schedule_interval = sched,
                         date              = datetime.date(datetime.now()))

    # Read in the template file and render it with template_dict values
    template_path = join(dirname(abspath(__file__)),'airflow.template.py')
    with open(template_path,'r') as f:
        template_contents = f.read()

    dag_file_contents = Template(template_contents).render(**template_dict)

    # Write the contents of the dag file to the
    DAG_FOLDER = environ['DAG_FOLDER']
    new_dag_file_pth = join(DAG_FOLDER, 'test.py')
    with open(new_dag_file_pth, 'w') as f:
        f.write(dag_file_contents)
    # if not exists(join(DAG_FOLDER,str(self.hash)+'.py')) or True:
    #     # move old model to archive
    #     dag_files = glob(join(DAG_FOLDER,'*.py'))
    #     if dag_files:
    #         assert len(dag_files) == 1, f'Why are is more than one file in the Dag folder: {DAG_FOLDER}?'
    #         old_dag_file_pth = dag_files[0]
    #         shutil.move(old_dag_file_pth,join(DAG_FOLDER,'archive'))
    #     new_dag_file_pth = join(DAG_FOLDER, str(self.hash)+'.py')
    #     with open(new_dag_file_pth, 'w') as f:
    #         f.write(dag_file_contents)

    print('Run Airflow!')
