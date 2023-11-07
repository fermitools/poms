---
layout: page
title: Archiving Proposal
---
* TOC
{:toc}
NOTE: This page is not terribly applicable since POMS 4.0.0, when we dropped individual job monitoring in POMS altogether. We've offloaded all that to Landscape, and we now
only track Submissions.

### Rationale

Right now, POMS is keeping info on every single production job ever run with POMS, and every file transferered by those jobs. This is not practical in the long term, and our design always included the idea of archiving/rolling such info to save space.

To allow archiving we need to convert our current Submission (Task), TaskHistory, Job, JobHistory, and JobFiles tables down to a condensed ArchivedSubmission, which will remember counts of things so that we can maintain statistics, but which will clean out large quantities of table space.

### What needs to be kept?

In order to roll up the tables above into an ArchivedSubmission, we need to know what information our users need about campaigns and submissions long term. We are assuming the following

* After some time window (which is how long? A month?), we no longer routinely need "Triage" level info about individual jobs anymore, rather we need is statistics information:
  * SAM projects (if any)
  * Job totals (Total Completed, Located, Removed, Totals by exit code)
  * File totals (Total delivered, Pending(?))
  * Efficiency histogram totals (11 bins, unknown, 0-9%, 10-19%, etc)
  * Simplified timeline of submission (4 times: idle, running, completed, located)
  * some sort of collapsed jobid info? (i.e. cluster-set, proc-range, schedd)


### Concerns

Do we need to roll off actual job info into some sort of store that we dump to tape or something
that we can get back if needed -- that is do we need an Un-Archive operation? I think our
users would like this -- esp. if this included archiving their job log tarballs, too?

[We could clone the Task,TaskHistory,Job,Jobhistory stuff for what we archive in a given batch into a sqlite db, and then we could un-archive a given Submission(Task) by pulling the sqlite file back off of DCache, opening it, and migrating the related info back into the main database...]

### Basic Idea

For any given Submisison(Task) id, we can Archive it by creating an ArchivedSubmission, computing the appropriate totals, etc. [optionally transferring detailed table info into an archive SQLite file] and doing a cascade-delete of the existing Task. We should do this in a single transaction(?) so that at any time we have either a Task with the given ID or an ArchivedSubmission of that ID.

Then all of our pages that show counts/totals for campaigns/submissions need to be able to get
the counts from both the ArchivedSubmissions and total up from Tasks associated with that
campaign, etc.