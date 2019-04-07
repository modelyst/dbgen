# External
from typing import Any
from unittest      import TestCase
from typing        import Any
from jsonpickle    import encode, decode # type: ignore

# Internal
from dbgen.test.test_objects    import make_model
from dbgen.test.test_generators import addgens

################################################################################

class TestSerialization(TestCase):
    '''To/From JSON instances for everything?'''
    def setUp(self) -> None:
        self.model = make_model()

    def check(self, x : Any) -> None:
        x_ = decode(encode(x))
        try:
            self.assertEqual(x,x_)
        except:
            import pdb;pdb.set_trace();assert False
    def test_all(self) -> None:
        '''
        FOR SOME REASON, MODEL IS SCREWED UP BY JSONPICKLE

        STUFF VERY LOW IN THE NESTING (USUALLY NEAR ENV OF SOME FUNC)
        GETS JUMBLED UP OR CONVERTED TO STRING

        DECODE(ENCODE(GEN)) WORKS BUT NOT DECODE(ENCODE(LIST[GEN]))
        (FOR THE LAST GENERATOR, GENS[0:6] CAN BE SERIALIZED AS LIST)
        '''
        m  : Any  = self.model

        addgens(self.model)

        objs = [m['movie'],m['rating'],  # Object
                ]

        for obj in objs:
            self.check(obj)
