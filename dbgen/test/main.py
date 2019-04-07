# External
from unittest import main
from sys      import exit
################################################################################
pre  = 'dbgen.test.tests.'
mods = ['UnitSimple','Serialization','UnitComplex','Integration']
args = dict(failfast = True, exit = False, warnings = 'ignore')

if __name__ == '__main__':

    for m in mods:
        print('\nTesting ',m)
        res = main(module=pre+m, **args) # type: ignore
        if res.result.errors or res.result.failures:
            exit()
