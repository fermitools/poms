---
layout: page
title: Testing Notes
---
* TOC
{:toc}
### Regression Tests
If you have setup your development virtualenv (see [Deployment with virtualenv]({{ site.url }}/docs/internals/deployment_with_virtualenv), and have poms checked out and installed in it, and are in your checked out copy, you can

    cd test
    pytest 2>&1 | tee pytest.out

to run the tests and keep a copy of any output in pytests.out

In theory all the tests should run clean; at the moment I think there is a workflow test that is failing.

### Testing poms client
The job_broker/poms_client subdirectory is arranged as a ups package (since that's how we distribute poms_client).

It has a "bin" subdirectory with command line client tools, and a lib directory with the library calls in back of them.

Your mission, should you choose to accept it is:

* set it up:
  * cd to your checked out copy
  * cd job_broker/poms_client
  * setup -. poms_client
* run the commands in bin/ with --help and
  * see if you can figure out how to use them
* create for a two-step workflow
  * job_types
  * launch_templates
  * campaign definitions
* check them
  * examine in the gui
  * make sure they are set as requested
* launch a job for the workflow in the GUI, make sure it runs
* maybe fix up the launcher from the ../../cron directory and add it
* use the above to launch jobs

For extra credit, write a python script that uses the library calls to perform the above sequence
(except the "examine in gui" bits)