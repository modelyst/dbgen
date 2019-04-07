from dbgen import (Model, Gen, PyBlock, Env, Import as Imp, defaultEnv)

from dbgen.test.test_funcs import load_db
#########################
def addgens(model : Model) -> None:
    ################################################################################
    Reviewer, Movie, Rating, Glob, Cast =                                       \
        map(model.get, ['reviewer', 'movie', 'rating', 'global', 'cast'])

    ############################################################################

    load_db_pb = PyBlock(load_db,
                    env = defaultEnv + Env(Imp('os','environ'),Imp('dbgen',ConnectInfo='Conn')),
                    outnames = ['stars', 'date', 'title', 'year', 'dir', 'rev'])

    rev_act =  Reviewer(insert = True, name=load_db_pb['rev'])
    add_things =                                                                \
        Gen(name    = 'add',
            desc    = 'Copies the DB from Stanford example',
            funcs   = [load_db_pb],
            actions = [Rating(insert = True,
                              stars  = load_db_pb['stars'],
                              date   = load_db_pb['date'],
                              movie  = Movie(insert = True ,
                                             title    = load_db_pb['title'],
                                             year     = load_db_pb['year'],
                                             director = load_db_pb['dir']),
                              reviewer = Reviewer(insert = True,
                                                  name=load_db_pb['rev']))])

    ########################################################################
    ########################################################################
    ########################################################################
    model.add([add_things])
