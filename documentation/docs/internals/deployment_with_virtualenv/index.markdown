---
layout: page
title: Deployment With Virtualenv
---
* TOC
{:toc}


First configure the system in accordance with a [Common Base Environment]({{ site.url }}/docs/internals/common_base_environment) to be shared with SAM and POMS projects, and then using a Python [virtualenv](https://virtualenv.pypa.io/en/stable/) area to bundle the dependent packages for POMS or SAM etc. along with it. This will also facilitate making
an RPM of the whole virtualenv for production installation via frozen RPMs.

### Base system

First your base system should be configured per the [Common Base Environment]({{ site.url }}/docs/internals/common_base_environment), which will use the "softwarecollections" tools to install current versions of python, gcc, etc.

### Creating the virtual environment

You should pick a location for the virtual environment; by convention this is usually in the home area of the "poms" account; but you can put it in your own home area for a personal development area. We'll be referring to the account area as $HOME.

    $ scl enable rh-python36 rh-postgresql96 bash
    bash-4.1$ which python
    /opt/rh/rh-python36/root/usr/bin/python
    bash-4.1$ cd $HOME
    bash-4.1$ mkdir $HOME/poms_venv
    bash-4.1$ virtualenv poms_venv
    New python executable in poms_venv/bin/python2
    Also creating executable in poms_venv/bin/python
    Installing setuptools, pip, wheel...done.
    bash-4.1$ exit
    setuptools needed to be upgrade to newer version in order to install packages later
    $ scl enable rh-python36 rh-postgresql96 bash
    $ source $HOME/poms_venv/bin/activate
    $ pip install -U pip setuptools


### Optional -- patch up virtual_env and scl enable bits

We can now make a script using scl_source that we can source to setup the whole shebang:

    echo " 
    scl_source enable rh-python36 
    scl_source enable rh-postgresql96 
    source $HOME/poms_venv/bin/activate" > $HOME/poms_venv/bin/scl_activate


### setting up the virtual environment

[Now if you did the optional previous section, you can skip the 'scl enable' part below]

    bash-4.11$ source $HOME/poms_venv/bin/scl_activate
    (poms_venv)bash-4.11$ 


### pip install dependencies

Now with the virtual environment setup, we can install our packages.

    If you don't have the libjpeg lib do:  
    sudo yum install libjpeg-turbo-devel
    check with the command:
    yum provides */libjpeg.so

    (poms_venv)bash-4.11$ for pkg in psycopg2 cherrypy sqlalchemy jinja2 python-crontab requests dogpile.cache pytest mock uwsgi
    do 
        pip install $pkg
    done

    Collecting psycopg2
    ...
    Successfully installed Logbook-1.1.0 dowser-py3-0.2.1 emport-1.2.0 infi.pyutils-1.1.3 packaging-16.8 pyparsing-2.2.0 z3c.recipe.scripts-1.0.1 zc.recipe.egg-2.0.3
    (poms_venv)bash-4.11$ 

Now, get a copy of the code from git and let the virtual environment know where it is located.

  
    cd to where ever you want to put the poms code
    git clone ssh://p-prod_mgmt_db@cdcvs.fnal.gov/cvs/projects/prod_mgmt_db poms
    cd poms
    pip install -e $PWD

Finally, we can run some tests:

    (poms_venv)bash-4.11$ cd poms 
    (poms_venv)bash-4.11$ export POMS_DIR=`pwd`
    (poms_venv)bash-4.11$ export PYTHONPATH=`pwd`:$PYTHONPATH
    (poms_venv)bash-4.11$ cd test
    (poms_venv)bash-4.11$ pytest 

### Files:
* <a href="/docs/files/scl_extract_setup" download>scl_extract_setup</a>
