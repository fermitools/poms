---
layout: page
title: Data Movement/Awareness Proposal
---
* TOC
{:toc}
Three major areas:

* Job-submission site choice based on data locations
* Out of workflow data placement / staging
* Data placement stages in workflows

Is there something else people are thinking of? Some sort of predictive dohickey?
it may not belong in POMS then...

### Site choice based on data locations (Jobs to Data)

This involves:

* Getting data disks/number of files there for a dataset (push to SAM, already a report we wanted?)
* Mapping those data disk locations to preferred sites (per-experiment table? also push to SAM?)
* If data is largely available at specific sites, splitting into multiple submissions, each with
  * Dataset subset on that site's data disks (but not elsewhere?)
  * Submission tagged to that site
* Mop-up submission for files not at those sites

This can be done independently of other options, and can be a new Campaign Stage flag, like split-type.

### Out of workflow data placement/staging

This involves:

* Finding Campaign Stages likely to launch soon:
  * Have cron launches pending
  * Depend on Campaigns that have jobs running
  * Users have hand-launched recently
* Finding next dataset those Campaign stages will get
* Taking appropriate action -- prestaging, transferring to particular sites

So we need an interative screen to let you restrict Campaign Stages involved (at least by exp, etc), and what action to take, and run it; plus we need a scheme to schedule these activities (i.e. via cron)

### Data placement stages in workflows

Need data movement stages to workflows. So this becomes a CampaignStage thats actually a data movement, rather than a job submission. Interesting option is if it is a small job submission under the covers...   

But basically, it would schedule a dataset file transfer (possibly by calling a SAM service, or by submitting a job that does sam_clone_dataset, or...) and waiting for it to complete. The only kludgy bit there is we still need to flag it as a transfer stage, because it's output dataset is the parent's output dataset for dependency purposes.