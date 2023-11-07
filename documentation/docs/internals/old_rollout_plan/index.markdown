---
layout: page
title: Old Rollout Plan
---
* TOC
{:toc}
### Overall

The plan is to build the system from the bottom up in several major releases.

* The initial release will support only batch-level tasks, and interfaces to SAM and FIFEmon, and will handle batch-level tasks currently being launched vi "cron" by the OPOS group -- keep-up processing/calibration/ etc. It will support the basic monitoring and post-mortem analysis screens for those tasks.
* A second major release will take on job submission, and handle campaign-level tasks which have already been approved and assigned to OPOS staff.
* The third major release will include the high-level task workflows involving approval, etc.

Of course, bugfix releases, etc. will occur in the interim.

### Deployment:

The development system is being deployed on Fermicloud (currently fermicloud045.fnal.gov). As we become ready for the 1.0 release, we will stand up a GP cluster virtual machine for the production instance. A development database will likely be on fnalpgsdev.

All software will be operating under a "poms" account on that system, with .k5login entries
for developers, etc.

Software will be in the Git repository of this Redmine project.

### Updates
Current status 2015-07-10

* Development database up, schema installed
* web instance is up on https://fermicoud045.fnal.gov:8080/poms/
* service status updates are occurring on 5 minute boundaries.
* web instance and status scraper are running under poms@fermicloud045.fnal.gov
* team members can log into poms@fermicloud045.fnal.gov, or can do
* development with standalone instances on their wordstations/laptops


Current status 2015-06-28

POMS - Production Operations Management System  
New VM created: fermicloud045.fnal.gov  
kx509 added: yum install krb5-fermi-getcert  
ups/upd added: yum install upsupdbootstrap-fnal  
added following users to products account: mgheith@FNAL.GOV, mengel@FNAL.GOV, illingwo@FNAL.GOV, swhite@FNAL.GOV, pbuitrag@FNAL.GOV  
git added: upd install git v1_8_5_3 "-G -c"  
cherrypy added: upd install cherrypy v3_2_4 "-G -c"  
python already there: do setup python v2_7_6 for newer version  
mako added: upd install mako v1_0_0 "-G -c"  
sam_web_client added: upd install sam_web_client v1_9 "-G -c"  
nose added: upd install nose v1_3_0 "-G -c"  
postgres client added: upd install postgres_client v9_3_5_p2_7 "-G -c"  
psycopg2 added: upd install psycopg2 v2_5_p2_7 "-G -c" //do not use this  
sqlalchemy added: upd install sqlalchemy v1_0_4 "-G -c"  
Jinja2 doesn't appear to have a upd so did [root@fermicloud045 ~]# easy_install Jinja2