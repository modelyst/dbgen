# External Modules
from typing import (Any, TYPE_CHECKING,
                    List     as L,
                    Union    as U,
                    Dict     as D,
                    Tuple    as T,
                    Union    as U)
from collections import OrderedDict
from networkx import DiGraph # type: ignore
import psycopg2 # type: ignore
import re
from jinja2 import Template
from io import StringIO
from random import getrandbits
# Internal Modules
if TYPE_CHECKING:
    from dbgen.core.schema import Obj, Rel
    Obj,Rel

from dbgen.core.funclike import ArgLike,Arg
from dbgen.utils.misc   import hash_, Base
from dbgen.utils.lists  import broadcast
from dbgen.utils.sql    import (Connection as Conn, sqlselect, addQs,
                                 sqlexecute,sqlexecutemany)
from dbgen.templates import jinja_env
'''
Defines the class of modifications to a database

There is a horrific amount of duplicated code in this file...... oughta fixit
'''
################################################################################

class Action(Base):
    """
    The purpose for this object is to make an easily serializable data structure
    that knows how to update the database (these methods could easily be for
    Model, but we don't want to send the entire model just to do this small thing)
    """
    def __init__(self,
                 obj    : str,
                 attrs  : D[str,ArgLike],
                 fks    : D[str,'Action'],
                 pk     : Arg    = None,
                 insert : bool   = False
                 ) -> None:

        self.obj    = obj.lower()
        self.attrs  = {k.lower():v for k,v in attrs.items()}
        self.fks    = {k.lower():v for k,v in fks.items()}
        self.pk     = pk
        self.insert = insert

        err = 'Cant insert %s if we already have PK %s'
        assert (pk is None) or (not insert), err%(obj,pk)
        assert isinstance(pk,(Arg,type(None))), (obj,attrs,fks,pk,insert)

    def __str__(self) -> str:
        n = len(self.attrs)
        m = len(self.fks)
        return 'Action<%s, %d attr, %d rel>'%(self.obj,n,m)

    ##################
    # Public methods #
    ###################
    def tabdeps(self) -> L[str]:
        '''All tables that are updated (they must already exist, is the logic)'''
        deps = []
        # Check if we are updating; if so we depend on
        if not self.insert: deps.append(self.obj)
        for fk in self.fks.values():
            deps.extend(fk.tabdeps())
        return deps

    def newtabs(self) -> L[str]:
        '''All tables that could be inserted into this action'''
        out = [self.obj] if self.insert else []
        for a in self.fks.values():
            out.extend(a.newtabs())
        return out

    def newcols(self, universe : D[str,'Obj']) -> L[str]:
        '''All attributes that could be populated by this action'''
        obj = universe[self.obj]
        out = [self.obj+'.'+a for a in self.attrs.keys()
                if (self.insert or (a not in obj.ids()))]
        for k,a in self.fks.items():
            if (self.insert or (k not in obj.id_fks())):
                out.extend([self.obj+'.'+k] + a.newcols(universe))
        return out

    def act(self,
            cxn  : Conn,
            objs : D[str,T[str,L[str],L[str]]],
            rows : L[dict]
           ) -> None:
        '''
        Top level call from a Generator to execute an action (top level is
        always insert or update, never just a select)
        '''
        self._load(cxn,objs,rows, insert = self.insert)
        # if self.insert:
        #     self._insert(cxn,objs,row)
        # else:
        #     self._update(cxn,objs,row)

    def rename_object(self, o : 'Obj', n :str) -> 'Action':
        '''Replaces all references to a given object to one having a new name'''
        a = self.copy()
        if a.obj == o.name:
            a.obj = n
        for k,v in a.fks.items():
            a.fks[k] = v.rename_object(o,n)
        return a

    ###################
    # Private methods #
    ###################

    def _getvals(self,
                 cxn  : Conn,
                 objs : D[str,T[str,L[str],L[str]]],
                 row  : dict,
                 ) -> T[L[int],L[list]]:
        '''
        Get a broadcasted list of INSERT/UPDATE values for an object, given
        Pyblock+Query output
        '''

        idattr,allattr = [],[]
        obj_pk_name,ids,id_fks = objs[self.obj]
        for k,v in sorted(self.attrs.items(),):
            val = v.arg_get(row)
            allattr.append(val)
            if k in ids:
                idattr.append(val)

        for kk,vv in sorted(self.fks.items()):
            if not vv.pk is None:
                val = vv.pk.arg_get(row)
            else:
                val, fk_adata = vv._getvals(cxn, objs, row)

            allattr.append(val)
            if kk in id_fks:
                idattr.append(val)

        idata,adata = broadcast(idattr),broadcast(allattr)
        if self.pk is not None:
            assert not idata, 'Cannot provide a PK *and* identifying info'
            pkdata = self.pk.arg_get(row)
            if isinstance(pkdata,int):
                idata_prime = [pkdata]
            elif isinstance(pkdata,list) and isinstance(pkdata[0],int): # HACKY
                idata_prime = pkdata
            else:
                raise TypeError('PK should either receive an int or a list of ints',vars(self))
        else:
            idata_prime = list(map(hash_,idata))

        if len(idata_prime) == 1: idata_prime *= len(adata) # broadcast

        lenerr = 'Cannot match IDs to data: %d!=%d'
        assert len(idata_prime) == len(adata), lenerr%(len(idata_prime),len(adata))
        return idata_prime, adata

    def _data_to_stringIO(self,
                          pk   : L[int],
                          data : L[list],
                          obj_pk_name : str,
                          )->StringIO:
        """
        Function takes in a path to a delimited file and returns a IO object
        where the identifying columns have been hashed into a primary key in the
        first ordinal position of the table. The hash uses the id_column_names
        so that only ID info is hashed into the hash value
        """
        # All ro
        output_file_obj = StringIO()
        cols = list(self.attrs.keys()) + list(self.fks.keys()) + [obj_pk_name]
        for i, (pk_curr, row_curr) in enumerate(zip(pk,data)):
            full_row         = [pk_curr]+list(row_curr)
            str_full_row     = map(str,full_row)
            str_full_row_esc = map(lambda x: x.replace("\t","\\t").replace('\n','\\n').replace('\r','\\r').replace('\\','\\\\'),str_full_row)
            output_file_obj.write('\t'.join(str_full_row_esc)+'\n')

        output_file_obj.seek(0)

        return output_file_obj

    def _load(self,
                cxn  : Conn,
                objs : D[str,T[str,L[str],L[str]]],
                rows : L[dict],
                insert : bool
              ) -> L[int]:
        '''
        Helpful docstring
        '''

        for kk,vv in self.fks.items():
            if vv.insert:
                val = vv._load(cxn,objs,rows, insert=True)

        obj_pk_name,ids,id_fks = objs[self.obj]
        pk,data = [], []
        for row in rows:
            pk_curr,data_curr = self._getvals(cxn,objs,row)
            pk.extend(pk_curr)
            data.extend(data_curr)


        io_obj = self._data_to_stringIO(pk, data, obj_pk_name)
        if not data: return []

        # Temporary table to copy data into
        # Set name to be hash of input rows to ensure uniqueness for parallelization
        temp_table_name = self.obj+'_temp_load_table_'+str(getrandbits(64))

        # Need to create a temp table to copy data into
        # Add an auto_inc column so that data can be ordered by its insert location
        create_temp_table = \
        """
        DROP TABLE IF EXISTS {temp_table_name};
        CREATE TEMPORARY TABLE {temp_table_name} AS
        TABLE {obj}
        WITH NO DATA;
        ALTER TABLE {temp_table_name}
        ADD COLUMN auto_inc SERIAL NOT NULL;
        """.format(obj = self.obj, temp_table_name = temp_table_name)

        cols         = [obj_pk_name]+list(sorted(self.attrs.keys())) + list(sorted(self.fks.keys()))
        escaped_cols = ['"'+col+'"' for col in cols]
        if insert:
            template = jinja_env.get_template('insert.sql.jinja')
        else:
            template = jinja_env.get_template('update.sql.jinja')

        first   = False
        update  = True
        fk_cols = self.fks.keys()
        template_args = dict(
            obj              = self.obj,
            obj_pk_name      = obj_pk_name,
            temp_table_name  = temp_table_name,
            all_column_names = cols,
            fk_cols          = fk_cols,
            first            = first,
            update           = update
        )
        load_statement = template.render(**template_args)

        get_ids_statement = \
        """
        SELECT
        	{obj_pk_name}
        FROM
        	{temp}
        ORDER BY
        	auto_inc ASC
        """.format(obj_pk_name = obj_pk_name, temp = temp_table_name)

        with cxn.cursor() as curs:
            curs.execute(create_temp_table)
            try:
                curs.copy_from(io_obj,temp_table_name,sep='\t',null='None',columns = escaped_cols)
            except psycopg2.errors.SyntaxError as exc:
                import pdb; pdb.set_trace()
                print('test')
            # Try to insert everything from the temp table into the real table
            # If a foreign_key violation is hit, we delete those rows in the
            # temp table and move on
            fk_fail_count = 0
            while True:
                if fk_fail_count==10:
                    if input('FK\'s have been violated 10 unique times, please confirm you want to continue (Y/y): ').lower() != 'y':
                        raise ValueError('User Canceled due to large number of FK violations')
                    else:
                        print('Continuing')
                # check for ForeignKeyViolation error
                try:
                    curs.execute(load_statement)
                    break
                except psycopg2.errors.ForeignKeyViolation as exc:
                    pattern ='Key \((\w+)\)=\(([\-\d]+)\) is not present in table \"(\w+)\"'
                    fk_name, fk_pk, fk_obj = re.findall(pattern, exc.pgerror)[0]
                    delete_statement = f'delete from {temp_table_name} where {fk_name} = {fk_pk}'
                    curs.execute(delete_statement)
                    print(f"ForeignKeyViolation: tried to insert {fk_pk} into FK column {fk_name} of {self.obj}. But no row exists with {fk_obj}_id = {fk_pk} in {fk_obj}.")
                    print(f"Moving on without inserting any rows with this {fk_pk}")
                    fk_fail_count += 1
                    continue

            # Get ids
            curs.execute(get_ids_statement)
            ids = [x[0] for x in curs.fetchall()]

        return ids


    def make_src(self) -> str:
        """
        Output a stringified version of action that can be run in an Airflow PythonOperator
        """
        attrs    = ','.join(['%s=%s'%(k,v.make_src(meta=True)) for k,v in self.attrs.items()])
        template = '''Load(obj= '{{ obj }}',attrs= dict({{attrs}}),fks=dict({{ fks }}),pk= {{ pk }},insert={{ insert }})'''
        fks      = ','.join(['%s=%s'%(k,v.make_src()) for k,v in self.fks.items()])
        pk       = None if self.pk is None else self.pk.make_src(meta=True)
        return Template(template).render(obj=self.obj,attrs=attrs,fks=fks,pk=pk,insert=self.insert)
