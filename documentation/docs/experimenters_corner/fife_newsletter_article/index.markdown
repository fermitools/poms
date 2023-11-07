---
layout: page
title: Fife Newsletter Article
---
* TOC
{:toc}
POMS stands for "Production Operations Management System", and it was initially developed for the OPOS group to help them effectively manage job submissions for multiple experiments.

Now we are working to make POMS into a tool that the experiments can use directly to help track their production computing. It:

* Lets experiment production users define "Campaign layers" of specific types of work, and group them together into larger Campaigns as needed.
* Tracks job submissions of multiple jobs for those campaign layers
* Automatically performs job submissions if so configured
* Can launch "recovery" jobs for files that didn't process properly the last time
* Can trigger launches in dependent campaign layers to process output of completed submissions
* Interfaces with the SAM data handling system to keep track of files and processing
* Provides a "Triage" interface to help debug what went wrong with failing jobs

POMS runs behind a web service interface that provides both interactive screens to users, and REST interfaces to scripts that interact with it. This means that experiments can use POMS through their web browser and configure and run their production code, or they can use the **poms_client** and **poms_jobsub_wrapper** tools to submit jobs and have POMS track them, without using the graphical interface for submission at all.

We are currently working with several FIFE experiments to start using POMS to help them manage and track their production processing, and hope to get to the rest in the next year.