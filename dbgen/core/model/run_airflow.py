# External imports
from typing  import TYPE_CHECKING, Any

import shutil
from glob import glob
from os      import environ
from os.path import join, abspath, dirname, exists
from datetime import datetime
from dbgen.core.airflow_plugin import GenOperator # type: ignore
# Internal Imports
if TYPE_CHECKING:
    from dbgen.core.model.model import Model

from dbgen.core.misc import ConnectInfo
from dbgen.templates import jinja_env

##################################
def run_airflow(self      : 'Model',
                sched     : str = '@once',
                nuke      : str = '',
                start     : str = '',
                until     : str = '',
                xclude    : str = '',
                only      : str = '',
                retry     : bool = False,
                serial    : bool = False,
                bar       : bool = True,
                clean     : bool = False,
                batch     : int  = None,
                **kwargs  : Any
                ) -> None:
    '''
    Create an airflow DAG, then execute it.
    '''
    # Validate input
    startErr = 'Starting generator ("start") must be a Generator name'
    assert not start or start in self.gens, startErr
    tillErr = 'Final generator ("until") must be a Generator name'
    assert not until or until in self.gens, tillErr
    xclude_ = set(xclude.split())
    only_   = set(only.split())
    for w in (only_ | xclude_):
        self._validate_name(w)

    from airflow.hooks.postgres_hook import PostgresHook # type: ignore
    # ping the database and check if we need to add Objs and Relations
    connection  = PostgresHook.get_connection(self.name)
    connI       = ConnectInfo.from_postgres_hook(connection)
    mconnection = PostgresHook.get_connection(self.name+'_log')
    mconnI      = ConnectInfo.from_postgres_hook(mconnection)


    if nuke:
        self.make_schema(conn=connI,nuke=nuke) # FULL NUKE

    # Check if the schema exists
    if not self.check_schema_exists(connI):
        raise ValueError('Your Schema doesn\'t exist yet, please run with --nuke=T the first time')

    # Make metatables
    #----------------
    run_id = self._make_metatables(mconn  = mconnI,
                                   conn   = connI,
                                   nuke   = nuke,
                                   retry  = False,
                                   only   = ' '.join(sorted(only_)),
                                   xclude = ' '.join(sorted(xclude_)),
                                   start  = start,
                                   until  = until,
                                   bar    = False)



    gen_hash_dict   = {gen_name: gen.hash for gen_name, gen in self.gens.items()}
    objs            = {oname   : (o._id, repr(o.ids()),repr(o.id_fks())) for oname, o in self.objs.items()}
    deps            = list(self._gen_graph().edges())
    template_kwargs = dict(user              = environ['USER'],
                           objs              = objs,
                           modelname         = self.name,
                           run_id            = run_id,
                           retry             = retry,
                           serial            = serial,
                           bar               = bar,
                           user_batch_size   = batch,
                           gen_hash_dict     = gen_hash_dict,
                           deps              = deps,
                           schedule_interval = sched,
                           date              = datetime.date(datetime.now()))

    dag_template = jinja_env.get_template('run_airflow.py.jinja')
    dag_file_contents = dag_template.render(**template_kwargs)

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
