from dbgen.core.schema import Obj, Attr, Rel
from dbgen.core.expr.sqltypes import Varchar, Int, Decimal, Text, Boolean



####
sample  = Obj('sample',attrs = [Attr('id',id = True)])
####
science = Obj('scientist',attrs=[Attr('firstname',Varchar()),
                                   Attr('lastname',Varchar()),
                                   Attr('ssn',id=True,desc='social security')])
####
proced = Obj('procedures',attrs=[Attr('procedure_name',Varchar(),id=True)])

####
hist   = Obj('history', "mapping table",
             attrs=[Attr('step',id=True,desc='order of operations'),
                    Attr('timestamp',Varchar()),
                    Attr('step_divides_ssn', Boolean())])

r1 = Rel('sample',   'history',              id=True)
r2 = Rel('expt_type','history','procedures', id = True)
r3 = Rel('operator', 'history','scientist')

####
hd   = Obj('history_detail','RDF triplestore',
           attrs = [Attr('name',Varchar(),id=True),
                    Attr('value',Text()),
                    Attr('dtype',Varchar())])

r4 = Rel('history', 'history_detail', id = True)
#####
elec = Obj('electrode','Either an anode or cathode',
           attrs = [Attr('composition',Varchar())])
r5   = Rel('sample','electrode',id=True)
#####
a,c = Obj('anode'),Obj('cathode')
r6,r7 = [Rel('electrode',x,id=True) for x in ['anode','cathode']]
#####
fc = Obj('fuel_cell','Combination of a particular anode and cathode sample during an expt',
         attrs=[Attr('expt_id',id=True),
                Attr('electrolyte',Varchar()),
                Attr('capacity',Decimal()),
                Attr('timestamp',Varchar()),
                Attr('calc_anode',Boolean())])
r8,r9= [Rel(x,'fuel_cell') for x in ['anode','cathode']]
#####
ec = Obj('electrode_composition','mapping table',
          attrs=[Attr('frac',Decimal())])
r10,r11= [Rel(x,'electrode_composition') for x in ['electrode','element']]
#####
elem = Obj('element', "Atomic elements",
           attrs = [Attr('atomic_number',id=True),
                    Attr('symbol',Varchar()),
                    Attr('name',Varchar()),
                    Attr('mass',Decimal())])

all = [sample,science,proced,hist,hd,elec,a,c,fc,elem,ec,
       r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11]
