---
layout: page
title: Campaign Task Files Help
---
* TOC
{:toc}
This is a table for the given time window (default 1 day) for the given campaign.  
It shows each job submission, and counts of files handled by each. Each count  
is a click through to show the list of files, and the SAM query to get the list.  

The page has the following columns:

##### jobsub_job_id

Jobid of the submission -- click through to see the time bars for the jobs, and to click through to a triage page for the jobs.

##### project

SAM Project name (or playlist file?) for the submission

##### date

date of the submission

##### submitted

Number of files in dataset/playlist.

##### delivered (SAM:logs)

Files delivered according to SAM and according to the log scraper.

##### consumed

Files listed as consumed in SAM

##### skipped

Files listed as skipped in SAM

##### unk.

##### w/some kids declared

Files we have some outputs declared for

##### w/all kids declared

Files we have some outputs declared for

##### w/kids inflight

Count of files "in flight" -- that is output files we saw in the logs that are not declared in SAM yet.

##### w/kids located¶

Count of files with all kids declared with locations.

##### pending

Count of files without all kids declared with locations.