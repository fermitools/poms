---
layout: page
title: Deploying with Spack
---
* TOC
{:toc}

Our production instances are now deployed with Spack; The procedure is:

### Bootstrap a Spack instance

Download a copy of the spack_infrastructure boostrap script, and assuming you want the spack area in /path/to/spack, run

    spackdir=/path/to/spack
    wget https://github.com/FNALssi/spack-infrastructure/raw/v2.19.0_release/bin/bootstrap
    bash bootstrap $spackdir > bootstrap.log 2>&1


### Install poms in spack

Setup the spack instance:

    source $spackdir/setup-env.sh
    spack env create poms_develop
    spack env activate poms_develop
    spack add poms@develop
    spack add py-supervisord
    spack add py-uwsgi
    spack add py-pytest
    spack add py-black
    spack add py-coverage
    spack add py-mock
    spack install


Or for a fixed release, create and activate an environment with the release in the name (i.e. poms_340 or some such) and

    spack add poms@4.3.0

Or whichever release instead of develop.

The last batch of those packages are for running the regression tests.

### Make a convenience link

    cd ~poms
    spack find --paths poms
    ln -s <path from above> poms


### Convert to a checked out area

If you checked out a tagged release, we want replace the poms area with a checked out copy to  
track hotfixes. If you checked out develop, it should already be a checked out copy.

    spack find --paths poms
    cd <path from above>
    cd ..
    git clone ssh://p-prod_mgmt_db@cdcvs.fnal.gov/cvs/projects/prod_mgmt_db 
    cd prod_mgmt_db
    git checkout vx_y_z
    git branch hotfix/vx_y_z
    git checkout hotfix/vx_y_z
    cd ..
    mv <last component of spack find path> old
    mv prod_mgmt_db <last component of spack find path>
    rm -rf old


### Setup supervisord

Last but not least, grab a copy of the supervisord config from poms's home area, and
adjust paths for your install, and grab a copy of the configs from ~poms/config/poms
on the development box (assumming you're going to use the existing dev database) and
similarly adjust for your paths.

### Using your instance:

Once it's all installed you just:

    source $spackdir/setup-env.sh
    spack env activate poms_develop

To make your area active, and then have supervisord start everything.