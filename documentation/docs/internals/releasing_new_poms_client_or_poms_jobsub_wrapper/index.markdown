---
layout: page
title: Releasing new poms_client or poms_jobsub_wrapper
---
* TOC
{:toc}

Poms_client is currently a ups package, distributed via upd;  
so the easiest is to cut it from a node with /grid/fermiapp/products or /cvsfs/fermilab.opensciencegrid.org/products
mounted; or you can install a ups/upd bootstrap tarball on any node.

### check out POMS

On a node with the common products mounted

    git clone ssh://p-prod_mgmt_db@cdcvs.fnal.gov/cvs/projects/prod_mgmt_db
    cd prod_mgmt_db/poms_client
    setup -. poms_client

Now you have the checked out copy setup, and you can run any tests you need to.  
We should be able to run bash tests/Test_client.sh -v but it has bit-rotted and needs fixing.

### add to fnkits with upd addproduct

Next put it up on fnkits to be installable; siting in the poms_client direcory:

    upd addproduct -. poms_client vx_y_z -0 


### install into common products area

    ssh products@fnkits.fnal.gov
    . /grid/fermiapp/products/common/etc/setups
    setup upd
    upd install -j poms_client vx_y_z


### add ups "chains" so people and programs use it:

    ups declare poms_client  vx_y_z -0 -g current -g poms41


## Releasing new poms_jobsub_wrapper

Poms_jobsub_wrapper is currently a ups package, distributed via upd;  
so the easiest is to cut it from a node with /grid/fermiapp/products or /cvsfs/fermilab.opensciencegrid.org/products
mounted; or you can install a ups/upd bootstrap tarball on any node.

### check out POMS

On a node with the common products mounted

    git clone ssh://p-prod_mgmt_db@cdcvs.fnal.gov/cvs/projects/prod_mgmt_db
    cd prod_mgmt_db/poms_jobsub_wrapper
    setup -. poms_jobsub_wrapper

Now you have the checked out copy setup, and you can run any tests you need to.

### add to fnkits with upd addproduct

Next put it up on fnkits to be installable; siting in the poms_jobsub_wrapper direcory:

    upd addproduct -. poms_jobsub_wrapper vx_y_z -0


### install into common products area

    ssh products@fnkits.fnal.gov
    . /grid/fermiapp/products/common/etc/setups
    setup upd
    upd install -j poms_jobsub_wrapper vx_y_z

    upd addproduct -. poms_jobsub_wrapper vx_y_z -0


### install into common products area

    ssh products@fnkits.fnal.gov
    . /grid/fermiapp/products/common/etc/setups
    setup upd
    upd install -j poms_jobsub_wrapper vx_y_z


### add ups "chains" so people and programs use it:

    ups declare poms_jobsub_wrapper  vx_y_z -0 -g current -g poms41
