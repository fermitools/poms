---
layout: page
title: Big Picture
---
* TOC
{:toc}
### Components

So in the Big Picture, POMs is now down to a few components:

* A Database for configuration and state
* Agents to report on sumbissions
* Cron-triggered events
* A web service to tie them together

![POMS Big Picture]({{ site.url }}/docs/images/poms_big_picture.png)

### How does POMS know whats happening?

In the Big Picture, we find out about Submissions that claim to have associated POMS entities from Landscape via our submissions_agent (which talks to the Landscape LENS service) and report their status to the webservice, which records them in the database. (Currently we actually run 2 agents, one that talks to LENS and another that runs condor_q, but we're hoping to be able to drop the latter). This provides the information on Submissions, when they start, complete, etc. to POMS.


### How does "workflow" happen.

POMS allows automatically triggered launches of dependencies when submissions are "done". Internally this is represented with a non-Condor based status called "Located", indicating the output files from the submission have locations in SAM and are available to use.

Currently this all happens when cron calls "wrapup_tasks" in the POMS code every few minutes. wrapup_tasks:

* gets a list of all "Completed" status submissions
* Based on the completion type:
  * if it's "complete" we're done, put it on the list to make Located
  * if it's "located",
    * and we have a SAM project associated:
      * make a dataset of the children of our input project's files
      * get a snapshot of our input project
      * get a count of the dataset, and count of delivered files
    * if it's "located" but no SAM project
      * assume it generated a poms_depends_nnn_1 dataset for us
      * count the dataset, and the number of jobs in the submission (mooch from launch command)
    * if our counts above meet our threshold percentage (children with locs vs delivered files) put it on the list to make Located
    * (this code is to be cleaned up for different campaign stage types)
      * submissions on the list to be made Located:
    * are thus updated in the database
    * next of any Recovery jobs are launched
    * otherwise all Dependent CampaignStages are launched.


### Object/db Model

Within the POMS database Submissions are associated with CampaignStages, which are part of Campaigns, and are
associated with particular Experimenters who created them, and E xperiments they belong to. CampaignStages within a given campaign can have Dependencies which result in "workflow" of automatically triggered submissions when they complete.

### Submission life cycle

When a Submission is made, either through the GUI or externally (external launches are still supported, just deprecated),
an initial report of the Submission is made via the poms_jobsub_wrapper/poms_client (which operates at the same level as the browser in the diagram) requesting a submission_id to put in the job's environment/ClassAds. This is the only way POMS knows about Submissions. If they haven't been registered, reports about them from the submissions_agent are ignored.