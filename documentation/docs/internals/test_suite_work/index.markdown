---
layout: page
title: Test Suite Work
---
* TOC
{:toc}
If you're reading this, you've probably been assigned to help add to the POMS test suite.

I'm assuming here that you have already setup your own development POMS instance somewhere,
either on your desktop, laptop, or on a FermiCloud node.

## Test Framework: pytest

We're going to be using the pytest framework. This means two things:

1. You should read the pytest introduction and possibly more of the full pytest documentation
2. You should also read up a bit on Mock
3. You need to add pytest to your ups products for your POMS area, so go to your development instance and

       setup upd
       upd install -G -c pytest v3_0_5

4. Now if you go to your checked out copy of POMS, git pull; setup -. poms you should be able to cd test; pytest test_AccessPOMS.py to run one of the test modules.


## Anatomy of a test module

So if we look, for example, at that test_AccessPOMS.py module, we see that the bottom is test routines that are generally pretty short, and the top is setup stuff, which is the part that really needs explaining.

### Parts

Basically, in our current architecture, we pass a lot of things down into the routines in AccessPOMS.py, TaskPOMS.py, etc. from our cherrypy/sqlalchemy environment; so if we want to run a test outside of that environment we need to pass in compatible parts. So there are a couple here:

The actual Poms objects -- many objects in the poms hierarchy have a reference to the poms_service class stored away as self.poms_service so they can call around to other modules which have instances in that poms_service class. So for testing we have the "mock_poms_service" class, which just contains one each of our classes and initializes them with references back to itself. We find our actual methods to test then as mps.whateverPOMS.method(..)

dbhandle -- we pass in the dbhandle from our cherrypy/sqlachemy session management; we use a DBHandle class here to find that, which gets our config info from poms.ini and connects to the database.

logging -- rather than cherrypy.log we use the logging module to get a logger instance, and pass its "info" method in.

session/gethead -- we fake up various parts of sessions using Mock/MagicMock to return answers it expects. In the Access methods we pass in the gethead method to return headers, in other modules we mock()-up something that always says we're authorized. So here in test_AccessPOMS.py, we are mocking up different versions of gethead that return different IP address/user/combinations. to test the access routines.

If we look at test_CampaignsPOMS.py, we have more faking to do; we use our mock_job.py module to pretend jobs are running and agents are reporting updates, and we test that things happen.