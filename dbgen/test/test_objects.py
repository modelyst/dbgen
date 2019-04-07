# Internal
from dbgen import Int, Varchar, Model, Obj, Attr, Rel, Boolean
##############################################################################

reviewer = Obj('reviewer', 'Movie reviewers',
               attrs = [Attr('name',Varchar(),id=True),
                        Attr('income',Int()),
                        Attr('accredited',Boolean(),
                              desc='Special distinction for real pros')])
##############
movie = Obj('movie','Cinema',
            attrs = [Attr('title',Varchar(),id=True),
                     Attr('year',Int()),
                     Attr('director',Varchar())])
##############
rating = Obj('rating','Movie reviews',
            attrs = [Attr('date',Varchar(),id=True,
                            desc='We encode into the schema the fact that the '
                                 'same reviewer will not review the same movie '
                                 'twice in the same day, thus we can ID reviews '
                                 'by their date'),
                    Attr('stars',Int(),desc='It may be necessary in the "finished '
                                            'product", but it\'ts not identifying')])

rate_rels = [Rel('reviewer','rating',id=True),
             Rel('movie',   'rating',id=True)]
##############
glob = Obj('global','Global properties of DB --- At most just one row due to no IDs!',
            attrs = [Attr('database_admin',Varchar())])
glob_rels = [Rel('worst_movie','global','movie'),
             Rel('best_movie','global','movie')]
##############
cast = Obj('cast','ORDERED list of actors in a movie - many-to-one relationship with Movie',
            attrs = [Attr('ind',Int(),id=True,desc='Order in the billing'),
                     Attr('name',Varchar(),desc='Actor name')])

cast_rels = [Rel('movie','cast',id=True)]
##############
actor = Obj('actor','People who act in movies',
            attrs = [Attr('name',Varchar(),id=True),
                     Attr('born',Int(),desc='Birth year'),
                     ])
act_rels = [Rel('breakout_role','actor','movie',
                desc = 'The first movie they starred in that got 5 stars from '
                        'an accreditted reviewer'),
            Rel('favorite','actor','movie',desc='Actors have favorite movies too!')]

##############
objs = [reviewer, movie, rating, glob, cast, actor]

rels= rate_rels + glob_rels + cast_rels + act_rels


######################################################################

def make_model() -> Model:
    '''Create empty model, then add all objects'''
    m = Model('test')
    m.add(objs+rels) # type: ignore
    return m

if __name__ == '__main__':
    m = make_model()
