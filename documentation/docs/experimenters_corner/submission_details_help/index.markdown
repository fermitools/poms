---
layout: page
title: Submission Details Help
---
* TOC
{:toc}
This page gives you pretty much all the available information about a given Submission.

The top box gives you the internal Submission id number, the jobsub-job-id, and the name, id and link to the Campaign Stage and Campaign the submission is associated with.

The Information box tells you how/why it was launched, and has links to the launch script output, and possibly links to related information in Landscape, SAM, and job logs on the jobsub_submit servers.

The Actions box has buttons/forms for various actions depending on the status of the submission:

* If the submission is Idle or Running, buttons to hold/release/kill the running jobs
* If the submission is Completed but not Located a "fast forward" button to kick it ahead to Located
* If the submission is Located, buttons to re-launch the job entirely
* If teh submission is completed or Located, a one-line form to launch a recovery job
  * A pulldown to pick the type of recovery
  * Then a button to actually fire the launch
  * Many of these recovery types only work if the submission had a related SAM project.

The History box shows the times when POMS detected state transitions of the submission

The remaining boxes show details of the snapshots of the Submission, Campaign Stage, Job Type and Login/Setup associated with it made at the time of the launch (as opposed to what they may be set to now..)