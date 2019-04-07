# External Modules
from typing     import List,Any
from time       import sleep
from warnings   import filterwarnings
from psycopg2   import Error  # type: ignore
from psycopg2.extras import DictCursor               # type: ignore

"""
Tools for interacting with databases
"""

Connection = Any
##############################################################################
# Interface with DB
#------------------
def sub(q:str,xs:list)->str:
    """
    Subsitute binds for debugging
    """
    def s(x:Any)->str:
        if isinstance(x,str):
            return "'%s'"%x
        elif x is None:
            return 'NULL'
        else:
            return x
    try:
        return q.replace('%s','{}').format(*map(s,xs))
    except:
        print(q,q.count('%s'),len(xs))
        import pdb;pdb.set_trace()
        raise ValueError('Subsitution error')

###########
# Shortcuts
###########

def mkInsCmd(tabName : str, names : List[str]) -> str:
    dup    =  ' ON CONFLICT (uid) DO NOTHING'
    ins_names = ','.join(['"%s"'%(n) for n in names])
    fmt_args  = [tabName,ins_names,','.join(['%s']*len(names)),dup]
    return "INSERT INTO {} ({}) VALUES ({}) {}".format(*fmt_args)

def mkUpdateCmd(tabName  : str,
                names   : List[str],
                keys    : List[str]
                ) -> str:
    fmt_args = [tabName,addQs(names,','),addQs(keys,' AND ')]
    return "UPDATE {} SET {} WHERE {}".format(*fmt_args)

def mkSelectCmd(tabName:str,get:List[str],where:List[str])->str:
    fmt_args = [','.join(get),tabName,addQs(where,' AND ')]
    return "SELECT {} FROM {} WHERE {}".format(*fmt_args)

##############################################################################

def select_dict(conn : Connection, q : str, binds : list = []) -> List[dict]:
    # print('SELECTING with: \n'+sub(q,binds))
    with conn.cursor(cursor_factory=DictCursor) as cxn:
        cxn.execute(q,vars=binds)
        return cxn.fetchall()

def sqlselect(conn : Connection, q : str, binds : list = []) -> List[tuple]:
    #print('\n\nSQLSELECT ',q)#,binds)
    with conn.cursor() as cxn: # type: ignore
        cxn.execute(q,vars=binds)
        return cxn.fetchall()

def sqlexecute(conn : Connection, q : str, binds : list = []) -> None:
    with conn.cursor() as cxn: # type: ignore
        while True:
            try:
                cxn.execute(q,vars=binds)
                break
            except Error as e:
                if e.args[0] in [1205,1213]: # deadlock error codes
                    sleep(10)
                else:
                    raise Error(e)

def sqlexecutemany(conn:Connection,q:str,binds:List[list]) -> None:
    #print('\n\nexecutemany : \n'+q,binds)
    #print('\n\n(with substitution)\n',sub(q,binds[0]))
    with conn.cursor() as cxn: # type: ignore
        #cxn.execute("SET SESSION auto_increment_offset = 1")
        #cxn.execute("SET SESSION auto_increment_increment = 1")

        while True:
            try:
                cxn.executemany(q,vars=binds)
                break
            except Error as e:
                if  e.args[0] in [1205,1213]: # deadlock error codes
                    sleep(10)
                else:
                    raise Error(e)


def addQs(xs:list,delim:str)->str:
    """
    Ex: ['a','b','c'] + ',' ==> 'a = %s, b = %s, c = %s'
    """
    return delim.join(['{0} = %s'.format(x) for x in xs])
