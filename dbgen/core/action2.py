# External Modules
from typing import (Any, TYPE_CHECKING,
                    List     as L,
                    Union    as U,
                    Dict     as D,
                    Tuple    as T,
                    Union    as U)
from collections import OrderedDict
from networkx import DiGraph # type: ignore
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
    insert_template = \
        """
INSERT INTO {{obj}}
({% for column in all_column_names %}{{column}}{{ ", " if not loop.last }}{% endfor %})
SELECT
{% for column in all_column_names %}{{column}}{{ "," if not loop.last }}
{% endfor %}
FROM (
  SELECT
    {% for column in all_column_names %}{{column}},
    {% endfor %}
    ROW_NUMBER() OVER (PARTITION BY {{obj_pk_name}}
               ORDER BY auto_inc {{ "ASC" if first else "DESC"}}) AS row_number
  FROM
    {{temp_table_name}}) AS X
WHERE
  row_number = 1 ON CONFLICT ({{obj_pk_name}})
  DO
  UPDATE
  SET
  {% if not update %}
    {{obj_pk_name}} = excluded.{{obj_pk_name}}
  {% else %}
    {% for column in all_column_names %}{{column}} = excluded.{{column}}{{ "," if not loop.last }}
    {% endfor %}
  {% endif %}
  RETURNING
    {{obj_pk_name}}
        """
    update_template = \
        """
UPDATE
    {{obj}}
SET
    {% for column in all_column_names %}{{column}} = {{temp_table_name}}.{{column}}{{ "," if not loop.last }}
    {% endfor %}
FROM
    {{temp_table_name}}
WHERE
    {{obj}}.{{obj_pk_name}} = {{temp_table_name}}.{{obj_pk_name}};
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
            objs : D[str,'Obj'],
            rows  : L[dict]
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
                 objs : D[str,'Obj'],
                 row  : dict,
                 ) -> T[L[int],L[list]]:
        '''
        Get a broadcasted list of INSERT/UPDATE values for an object, given
        Pyblock+Query output
        '''

        idattr,allattr = [],[]
        obj = objs[self.obj]
        for k,v in self.attrs.items():
            val = v.arg_get(row)
            allattr.append(val)
            if k in obj.ids():
                idattr.append(val)

        for kk,vv in self.fks.items():
            if not vv.pk is None:
                val = vv.pk.arg_get(row)
            else:
                val, fk_adata = vv._getvals(cxn, objs, row)

            allattr.append(val)
            if kk in obj.id_fks():
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
            full_row = [pk_curr]+list(row_curr)
            output_file_obj.write('\t'.join(map(str,full_row))+'\n')

        output_file_obj.seek(0)

        return output_file_obj

    def _load(self,
                cxn  : Conn,
                objs : D[str,'Obj'],
                rows : L[dict],
                insert : bool
              ) -> L[int]:
        '''
        Helpful docstring
        '''

        for kk,vv in self.fks.items():
            if vv.insert:
                val = vv._load(cxn,objs,rows, insert=True)

        obj = objs[self.obj]
        pk,data = [], []
        for row in rows:
            pk_curr,data_curr = self._getvals(cxn,objs,row)
            pk.extend(pk_curr)
            data.extend(data_curr)
        io_obj = self._data_to_stringIO(pk, data, obj._id)
        if not data: return []


        # Temporary table to copy data into
        # Set name to be hash of input rows to ensure uniqueness for parallelization
        temp_table_name = self.obj+'_temp_load_table_'+str(getrandbits(64))

        # Need to create a temp table to copy data into
        # Add an auto_inc column so that data can be ordered by its insert location
        create_temp_table = \
        """
        DROP TABLE IF EXISTS {temp_table_name};
        CREATE TABLE {temp_table_name} AS
        TABLE {obj}
        WITH NO DATA;
        ALTER TABLE {temp_table_name}
        ADD COLUMN auto_inc SERIAL NOT NULL;
        """.format(obj = self.obj, temp_table_name = temp_table_name)
        sqlexecute(cxn,create_temp_table)

        cols        = [obj._id]+list(self.attrs.keys()) + list(self.fks.keys())

        if insert:
            template = self.insert_template
        else:
            template = self.update_template

        first = False
        update = True
        template_args = dict(
            obj              = self.obj,
            obj_pk_name      = obj._id,
            temp_table_name  = temp_table_name,
            all_column_names = cols,
            first            = first,
            update           = update
        )
        load_statement = Template(template).render(**template_args)

        with cxn.cursor() as curs:
            curs.copy_from(io_obj,temp_table_name,null='None',columns = cols)

        sqlexecute(cxn,load_statement)
        get_ids_statement = \
        """
        SELECT
        	{obj_pk_name}
        FROM
        	{temp}
        ORDER BY
        	auto_inc ASC
        """.format(obj_pk_name = obj._id, temp = temp_table_name)
        ids = [x[0] for x in sqlexecute(cxn,get_ids_statement)]
        clean_up = \
        """
        DROP TABLE {temp_table_name}
        """.format(temp_table_name = temp_table_name)
        sqlexecute(cxn, clean_up)
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
