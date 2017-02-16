from setuptools import setup 
from setuptools.config import read_configuration

cfdict = {}

cfdict.update(read_configuration('setup.cfg'))

# workaround read_configuration/setup bug
cfdict['options']['package_data'][''] = cfdict['options']['package_data'][u''] 
del cfdict['options']['package_data'][u''] 

print cfdict['options']['package_data']

setup( **cfdict['options'])
