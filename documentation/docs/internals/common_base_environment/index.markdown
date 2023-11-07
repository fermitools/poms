---
layout: page
title: Common Base Environment
---
* TOC
{:toc}
To have the software we would like to support of toolset, rather than re-bundle Python and other packages as UPS products, we will take advantage of the Software Collections configurations in Scientific Linux.

### Setup

The setup is straigthfoward, assuming you have root permission to run yum; if not you can ask that the sysadmin for your system install the packages, below.


    yum install http://ftp.scientificlinux.org/linux/scientific/6x/external_products/softwarecollections/yum-conf-softwarecollections-2.0-1.el6.noarch.rpm

    yum install python27 rh-python36
    yum install devtoolset-6-gcc-c++ devtoolset-6-gcc-gdb-plugin
    yum install rh-postgresql96-postgresql-devel rh-postgresql96

Now we have a more recent python, postgres clients, and gcc tools we can setup in /opt, using the "scl enable" utility:

scl enable python27 bash

will start up a bash shell with the python27 stuff setup. We'll see later how to merge the setup info
from an "scl enable" with a virtualenv setup, so we can have just one setup mechanism rather than two.