# External imports
from typing import Any
from airflow import DAG # type: ignore

from datetime import datetime

# Internal Imports
from dbgen import Gen
from dbgen.core.airflow_plugin import GenOperator # type: ignore

# Written by mstatt

objs = dict( roots = ('roots_id', ['root', 'code', 'cluster', 'label'],[]), job = ('job_id', ['stordir'],[]), element = ('element_id', ['atomic_number'],[]), cell = ('cell_id', ['a1', 'a2', 'a3', 'b1', 'b2', 'b3', 'c1', 'c2', 'c3'],[]), struct = ('struct_id', ['raw'],[]), atom = ('atom_id', ['ind'],['struct']), struct_composition = ('struct_composition_id', [],['struct', 'element']), surface = ('surface_id', [],['struct']), bulk = ('bulk_id', [],['struct']), molecule = ('molecule_id', [],['struct']), calc = ('calc_id', ['dftcode', 'xc', 'pw', 'psp'],[]), calc_other = ('calc_other_id', ['kx', 'ky', 'kz', 'fmax', 'econv', 'dw', 'sigma', 'nbands', 'mixing', 'nmix', 'xtol', 'strain', 'gga', 'luse_vdw', 'zab_vdw', 'nelmdl', 'gamma', 'dipol', 'algo', 'ibrion', 'prec', 'lreal', 'lvhar', 'diag', 'spinpol', 'dipole', 'maxstep', 'delta', 'mixtype', 'bonded_inds', 'step_size', 'spring', 'cell_dofree', 'cell_factor', 'energy_cut_off', 'optimizer'],[]), relax_job = ('relax_job_id', [],['job']), vib_job = ('vib_job_id', [],['job']), vib_modes = ('vib_modes_id', ['ind'],['vib_job']), traj = ('traj_id', ['step'],['relax_job']), traj_atom = ('traj_atom_id', [],['traj', 'atom']),) # type: D[str,T[str,L[str],L[str]]]

# Define DAG
default_args ={
    'owner'             : 'mstatt',
    'start_date'        : datetime.strptime('2019-07-24','%Y-%m-%d'),
    'retries'           : 1,
    'backfill'          : False,
    'catchup'           : False
}

default_args.update()
with DAG('simple_catalog_model',schedule_interval = '@once', default_args = default_args) as dag:
    
    catjob  = GenOperator(
                               objs            = objs,
                               gen_name        = 'catjob',
                               gen_hash        = -2502287164175389259,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    storage  = GenOperator(
                               objs            = objs,
                               gen_name        = 'storage',
                               gen_hash        = 7355180330199339795,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    elems  = GenOperator(
                               objs            = objs,
                               gen_name        = 'elems',
                               gen_hash        = -6706585320776172371,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    elemzinfo  = GenOperator(
                               objs            = objs,
                               gen_name        = 'elemzinfo',
                               gen_hash        = -8760974875327155465,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    anytraj  = GenOperator(
                               objs            = objs,
                               gen_name        = 'anytraj',
                               gen_hash        = -1836366843694216132,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    jobmetadata  = GenOperator(
                               objs            = objs,
                               gen_name        = 'jobmetadata',
                               gen_hash        = 2337720371661156101,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    paramdict  = GenOperator(
                               objs            = objs,
                               gen_name        = 'paramdict',
                               gen_hash        = -5880071455617827291,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    parse_vib_results  = GenOperator(
                               objs            = objs,
                               gen_name        = 'parse_vib_results',
                               gen_hash        = -8109758054202136920,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    hash_log  = GenOperator(
                               objs            = objs,
                               gen_name        = 'hash_log',
                               gen_hash        = 7234962498496010259,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pop_rjob  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pop_rjob',
                               gen_hash        = 7564918596031786312,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    n_steps  = GenOperator(
                               objs            = objs,
                               gen_name        = 'n_steps',
                               gen_hash        = 4914924551511413458,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pop_traj  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pop_traj',
                               gen_hash        = -7938692660072554753,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    n_atoms  = GenOperator(
                               objs            = objs,
                               gen_name        = 'n_atoms',
                               gen_hash        = -2422460155570180942,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    traj_atom  = GenOperator(
                               objs            = objs,
                               gen_name        = 'traj_atom',
                               gen_hash        = -2587020479780179034,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    fmax  = GenOperator(
                               objs            = objs,
                               gen_name        = 'fmax',
                               gen_hash        = 7126658907678807606,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    atom  = GenOperator(
                               objs            = objs,
                               gen_name        = 'atom',
                               gen_hash        = 7175275335691454979,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    cell  = GenOperator(
                               objs            = objs,
                               gen_name        = 'cell',
                               gen_hash        = 2537719782746017254,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    kden  = GenOperator(
                               objs            = objs,
                               gen_name        = 'kden',
                               gen_hash        = -8045262328703519296,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    parse_rjob  = GenOperator(
                               objs            = objs,
                               gen_name        = 'parse_rjob',
                               gen_hash        = -5963370966605515102,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    parse_rjob_vasp  = GenOperator(
                               objs            = objs,
                               gen_name        = 'parse_rjob_vasp',
                               gen_hash        = 7769915653318249208,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    calc_relax  = GenOperator(
                               objs            = objs,
                               gen_name        = 'calc_relax',
                               gen_hash        = 4921295245372143933,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    calc_vib  = GenOperator(
                               objs            = objs,
                               gen_name        = 'calc_vib',
                               gen_hash        = 3765600308938289026,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    calco_relax  = GenOperator(
                               objs            = objs,
                               gen_name        = 'calco_relax',
                               gen_hash        = -6966264085936404817,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    calco_vib  = GenOperator(
                               objs            = objs,
                               gen_name        = 'calco_vib',
                               gen_hash        = 9106457268955404528,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    cellinfo  = GenOperator(
                               objs            = objs,
                               gen_name        = 'cellinfo',
                               gen_hash        = -3762419538044599219,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    systype  = GenOperator(
                               objs            = objs,
                               gen_name        = 'systype',
                               gen_hash        = 5056518428844237624,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    blk  = GenOperator(
                               objs            = objs,
                               gen_name        = 'blk',
                               gen_hash        = 1126934921464938439,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    mol  = GenOperator(
                               objs            = objs,
                               gen_name        = 'mol',
                               gen_hash        = 6205859151277537322,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    surf  = GenOperator(
                               objs            = objs,
                               gen_name        = 'surf',
                               gen_hash        = -5485936135516149544,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pointgroup  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pointgroup',
                               gen_hash        = 7194836915872849208,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    spacegroup  = GenOperator(
                               objs            = objs,
                               gen_name        = 'spacegroup',
                               gen_hash        = 1241071436044071295,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    countatoms  = GenOperator(
                               objs            = objs,
                               gen_name        = 'countatoms',
                               gen_hash        = -8007294081558336263,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    elemental  = GenOperator(
                               objs            = objs,
                               gen_name        = 'elemental',
                               gen_hash        = -8315583058744360709,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    vacuum  = GenOperator(
                               objs            = objs,
                               gen_name        = 'vacuum',
                               gen_hash        = -1422163548928863612,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    struct_comp  = GenOperator(
                               objs            = objs,
                               gen_name        = 'struct_comp',
                               gen_hash        = 4229427009597234739,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pop_vibjob  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pop_vibjob',
                               gen_hash        = -4869341276999480503,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pop_vib_modes  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pop_vib_modes',
                               gen_hash        = -6813327370802060536,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    parse_vib_job  = GenOperator(
                               objs            = objs,
                               gen_name        = 'parse_vib_job',
                               gen_hash        = 303671271054971005,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    vib_job_parent  = GenOperator(
                               objs            = objs,
                               gen_name        = 'vib_job_parent',
                               gen_hash        = 8581860270296957772,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    free_energy_surf  = GenOperator(
                               objs            = objs,
                               gen_name        = 'free_energy_surf',
                               gen_hash        = -1879244787581635418,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    free_energy_mol  = GenOperator(
                               objs            = objs,
                               gen_name        = 'free_energy_mol',
                               gen_hash        = 5189861978394036638,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pop_sys_type  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pop_sys_type',
                               gen_hash        = -94925690564139631,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pop_vib_struct  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pop_vib_struct',
                               gen_hash        = 3837039608207296935,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    catads  = GenOperator(
                               objs            = objs,
                               gen_name        = 'catads',
                               gen_hash        = -8886575717801352830,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    catads_names  = GenOperator(
                               objs            = objs,
                               gen_name        = 'catads_names',
                               gen_hash        = -6133866576341270472,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    cat_struct  = GenOperator(
                               objs            = objs,
                               gen_name        = 'cat_struct',
                               gen_hash        = -7260031470799030988,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    cat_facet  = GenOperator(
                               objs            = objs,
                               gen_name        = 'cat_facet',
                               gen_hash        = -3406448300428444197,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    
    pop_inittraj  = GenOperator(
                               objs            = objs,
                               gen_name        = 'pop_inittraj',
                               gen_hash        = 9180423392973654848,
                               run_id          = 1,
                               db_conn_id      = 'simple_catalog_model',
                               mdb_conn_id     = 'simple_catalog_model_log',
                               retry           = False,
                               serial          = False,
                               bar             = True,
                               user_batch_size = None
                              )
    

# Add dependencies

anytraj.set_upstream(catjob)

jobmetadata.set_upstream(catjob)

paramdict.set_upstream(catjob)

parse_vib_results.set_upstream(catjob)

hash_log.set_upstream(catjob)

pop_rjob.set_upstream(catjob)

parse_rjob.set_upstream(catjob)

parse_rjob_vasp.set_upstream(catjob)

calc_relax.set_upstream(catjob)

calc_vib.set_upstream(catjob)

calco_relax.set_upstream(catjob)

calco_vib.set_upstream(catjob)

pop_vibjob.set_upstream(catjob)

parse_vib_job.set_upstream(catjob)

vib_job_parent.set_upstream(catjob)

catads.set_upstream(catjob)

catads_names.set_upstream(catjob)

cat_struct.set_upstream(catjob)

cat_facet.set_upstream(catjob)

pop_inittraj.set_upstream(catjob)

catjob.set_upstream(storage)

elemzinfo.set_upstream(elems)

atom.set_upstream(elems)

struct_comp.set_upstream(elems)

parse_rjob.set_upstream(anytraj)

catads_names.set_upstream(anytraj)

pop_rjob.set_upstream(jobmetadata)

parse_rjob.set_upstream(jobmetadata)

pop_vibjob.set_upstream(jobmetadata)

jobmetadata.set_upstream(paramdict)

pop_vibjob.set_upstream(paramdict)

parse_vib_job.set_upstream(paramdict)

catads.set_upstream(paramdict)

cat_struct.set_upstream(paramdict)

cat_facet.set_upstream(paramdict)

pop_inittraj.set_upstream(paramdict)

pop_vib_modes.set_upstream(parse_vib_results)

vib_job_parent.set_upstream(hash_log)

n_steps.set_upstream(pop_rjob)

pop_traj.set_upstream(pop_rjob)

traj_atom.set_upstream(pop_rjob)

parse_rjob.set_upstream(pop_rjob)

parse_rjob_vasp.set_upstream(pop_rjob)

calc_relax.set_upstream(pop_rjob)

calco_relax.set_upstream(pop_rjob)

pop_sys_type.set_upstream(pop_rjob)

pop_vib_struct.set_upstream(pop_rjob)

pop_inittraj.set_upstream(pop_rjob)

n_atoms.set_upstream(pop_traj)

traj_atom.set_upstream(pop_traj)

fmax.set_upstream(pop_traj)

atom.set_upstream(pop_traj)

cell.set_upstream(pop_traj)

kden.set_upstream(pop_traj)

systype.set_upstream(pop_traj)

blk.set_upstream(pop_traj)

mol.set_upstream(pop_traj)

surf.set_upstream(pop_traj)

pointgroup.set_upstream(pop_traj)

spacegroup.set_upstream(pop_traj)

countatoms.set_upstream(pop_traj)

elemental.set_upstream(pop_traj)

vacuum.set_upstream(pop_traj)

struct_comp.set_upstream(pop_traj)

free_energy_mol.set_upstream(pop_traj)

pop_sys_type.set_upstream(pop_traj)

pop_vib_struct.set_upstream(pop_traj)

elemental.set_upstream(n_atoms)

fmax.set_upstream(traj_atom)

atom.set_upstream(traj_atom)

struct_comp.set_upstream(traj_atom)

fmax.set_upstream(atom)

struct_comp.set_upstream(atom)

kden.set_upstream(cell)

cellinfo.set_upstream(cell)

n_steps.set_upstream(parse_rjob)

pop_traj.set_upstream(parse_rjob)

traj_atom.set_upstream(parse_rjob)

n_steps.set_upstream(parse_rjob_vasp)

pop_traj.set_upstream(parse_rjob_vasp)

traj_atom.set_upstream(parse_rjob_vasp)

kden.set_upstream(calco_relax)

kden.set_upstream(calco_vib)

kden.set_upstream(cellinfo)

blk.set_upstream(systype)

mol.set_upstream(systype)

surf.set_upstream(systype)

spacegroup.set_upstream(systype)

elemental.set_upstream(systype)

pop_sys_type.set_upstream(systype)

pointgroup.set_upstream(mol)

free_energy_mol.set_upstream(mol)

vacuum.set_upstream(surf)

free_energy_mol.set_upstream(pointgroup)

elemental.set_upstream(countatoms)

parse_vib_results.set_upstream(pop_vibjob)

calc_vib.set_upstream(pop_vibjob)

calco_vib.set_upstream(pop_vibjob)

pop_vib_modes.set_upstream(pop_vibjob)

parse_vib_job.set_upstream(pop_vibjob)

vib_job_parent.set_upstream(pop_vibjob)

free_energy_surf.set_upstream(pop_vibjob)

free_energy_mol.set_upstream(pop_vibjob)

pop_sys_type.set_upstream(pop_vibjob)

pop_vib_struct.set_upstream(pop_vibjob)

free_energy_surf.set_upstream(pop_vib_modes)

free_energy_mol.set_upstream(pop_vib_modes)

vib_job_parent.set_upstream(parse_vib_job)

pop_sys_type.set_upstream(vib_job_parent)

pop_vib_struct.set_upstream(vib_job_parent)

free_energy_surf.set_upstream(pop_sys_type)

free_energy_mol.set_upstream(pop_sys_type)

free_energy_mol.set_upstream(pop_vib_struct)

catads_names.set_upstream(catads)


if __name__ == '__main__':

    catjob.execute({})

    storage.execute({})

    elems.execute({})

    elemzinfo.execute({})

    anytraj.execute({})

    jobmetadata.execute({})

    paramdict.execute({})

    parse_vib_results.execute({})

    hash_log.execute({})

    pop_rjob.execute({})

    n_steps.execute({})

    pop_traj.execute({})

    n_atoms.execute({})

    traj_atom.execute({})

    fmax.execute({})

    atom.execute({})

    cell.execute({})

    kden.execute({})

    parse_rjob.execute({})

    parse_rjob_vasp.execute({})

    calc_relax.execute({})

    calc_vib.execute({})

    calco_relax.execute({})

    calco_vib.execute({})

    cellinfo.execute({})

    systype.execute({})

    blk.execute({})

    mol.execute({})

    surf.execute({})

    pointgroup.execute({})

    spacegroup.execute({})

    countatoms.execute({})

    elemental.execute({})

    vacuum.execute({})

    struct_comp.execute({})

    pop_vibjob.execute({})

    pop_vib_modes.execute({})

    parse_vib_job.execute({})

    vib_job_parent.execute({})

    free_energy_surf.execute({})

    free_energy_mol.execute({})

    pop_sys_type.execute({})

    pop_vib_struct.execute({})

    catads.execute({})

    catads_names.execute({})

    cat_struct.execute({})

    cat_facet.execute({})

    pop_inittraj.execute({})
