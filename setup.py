from setuptools import setup 
from setuptools.config import read_configuration


# workaround read_configuration/setup bug
def fixdict(d):
    d[''] = d.get('',None)
    del d['']

cfdict = read_configuration('setup.cfg')

fixdict(cfdict['options']['package_data'])

setup( **cfdict['options'])
