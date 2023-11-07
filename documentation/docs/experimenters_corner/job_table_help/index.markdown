---
layout: page
title: Job Table Help
---
* TOC
{:toc}
The job table is a searchable list of jobs meeting various criteria.

## Job columns

##### info

The info column has a link to the triage page for that job, with more details,
access to log files, etc.

##### jobsub job id

The fifebatch job-id of the job in this row.

##### node name

The node if any (and if available) that this job most recently ran on. If we have not seen a report from this job about where it is running, it is "unknown".

##### cpu type

Cpu type from /proc/cpuinfo logged by the job; if we haven't received such a log message, it is "unknown".

##### host site

GLIDEIN_SITE from the condor_q listing of the job.

##### status

Status of the job; one of the base Condor states of Idle, Held, Running, Completed, or Running: with more information from the jobs ifdh logging, or Located, which is our own state indicating it is not only completed, but all of its output files have locations in the Data Handling System.

##### output files declared

Flag about whether all of this jobs output files have locations in the Data handling system.

##### output file names

list of output file names copied out from the job, scraped from ifdh log messages that ifdh cp generates.

##### user exe exit code

Exit code from the user script reported by the jobsub wrapper via ifdh log.

##### input file names

Input files copied in as recorded by the ifdh cp log messages.

## Task columns

##### input dataset

name if dataset input if known

##### output dataset

name of dataset describing output files if known

##### status

The task status is a rollup of the job statuses, as follows;

* if no jobs have shown up in condor_q output yet, the Task is "New"
* if all jobs are Idle, the Task status is Idle
* if any jobs are Running, the Task status is Running
* if all jobs are Completed but not all are Located, the Task is Completed
* if all jobs are Located, the task has Located status


##### project

Data handling system "Project" name (i.e. SAM project)

##### Campaign fields

##### experiment

Experiment whose job/task/campaign this is.

##### name

Name of the campaign

##### task definition id

Task definition of this job

##### vo role

Role in the vo this runs under

##### cs last split

Bookkeeping for splitting tasks from Campaigns

##### cs split type

Type of Campaign splitting done

##### cs split dimensions

Dimensions used to further split into multiple tasks