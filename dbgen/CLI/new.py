#External imports
import sys
from venv       import create           # type: ignore
from os         import mkdir, environ, system
from os.path    import join, exists
from shutil     import copyfile
from argparse   import ArgumentParser
'''
Initialize a dbgen model
'''
################################################################################

#Check to confirm that python3.6 or newer is being used
major_version, minor_version = sys.version_info[:2]
if major_version < 3 or minor_version < 6:
    raise Exception("Python 3.6 or a more recent version is required.")

root = environ['DBGEN_ROOT']
user = environ['USER']
################################################################################

class File(object):
    def __init__(self, pth : str, template : str)->None:
        self.pth = pth
        self.template = template
    def content(self,kwargs:dict)->str:
        with open(join(root,'dbgen/CLI/newfiles',self.template),'r') as f:
            return f.read().format(**kwargs)
    def write(self,pth : str,**kwargs:str)->None:
        with open(join(pth,self.pth),'w') as f: f.write(self.content(kwargs))

sch     = File('schema.py','schema')
ginit   = File('generators/__init__.py','ginit')
default = File('dbgen_files/default.py','default')
io      = File('generators/io.py','io')
man     = File('main.py','main')
data     = File('data/example.csv','data')
parse   = File('scripts/io/parse_employees.py','parse')
dev,log = [File('dbgen_files/%s.json'%x,x) for x in ['dev','log']]

files = [sch,ginit,default,man,io,parse,dev,log]
inits = ['','scripts/','scripts/io/']
dirs  = ['generators','scripts','data','dbgen_files','scripts/io',
        'dbgen_files/storage','dbgen_files/tmp']


################################################################################
parser = ArgumentParser(description  = 'Initialize a dbGen model', allow_abbrev = True)
parser.add_argument('pth',type  = str, help = 'Root folder')
parser.add_argument('name',type = str, help = 'Name of model')
parser.add_argument('env',default='.env/bin/activate',type = str, help = 'Name of model')
################################################################################
envvars = dict(
    MODEL_STORAGE = 'dbgen_files/storage',
    MODEL_TEMP    = 'dbgen_files/tmp',
    MODEL_ROOT    = '')
################################################################################

def main(pth : str, name : str, env : str) -> None:
    '''
    Initialize a DbGen model
    '''
    if exists(pth): print(pth,' already exists'); return
    mkdir(pth)

    for dir in dirs:  mkdir(join(pth,dir))
    for i   in inits: system('touch '+join(pth,i+'__init__.py'))
    for fi  in files: fi.write(pth, model = name, user = user)

    # Create virtual environment
    env  = join(pth,env)
    reqs = join(root,'requirements.txt')
    create(join(pth,'.env'),with_pip = True,symlinks = True, clear = True)
    system('source '+env+'; pip install -r '+reqs)
    copyfile(reqs,join(pth,'requirements.txt'))
    with open(env,'a') as f:
        for k,v in envvars.items():
            f.write('\n\nexport {}={}/{}'.format(k,pth,v))


if __name__ == '__main__':
    args = parser.parse_args()
    main(args.pth,args.name, args.env)
