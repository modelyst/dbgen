from typing import List as L, Tuple as T
from os import environ

from dbgen.core.misc import ConnectInfo as Conn



def load_db()->T[L[int],L[str],L[str],L[int],L[str],L[str]]:
    '''Returns stars, date, movie, year, director, reviewer'''
    from os.path import join
    from sqlite3 import connect
    pth = join(environ['DBGEN_ROOT'],'dbgen/test/rating.db')


    q = '''SELECT stars,ratingDate,title,year,director,name
            FROM rating JOIN reviewer USING (rID)
                        JOIN movie    USING (mID)'''

    with connect(pth) as cxn: # type: ignore
        try:

            query_out =  cxn.execute(q)

            s,rd,m,y,d,r = map(list,zip(*query_out))
        except Exception as e:
            print(e)
            import pdb;pdb.set_trace()
    return s,rd,m,y,d,r # type: ignore


if __name__=='__main__':
    load_db()
