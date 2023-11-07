---
layout: page
title: Technical Design
---
* TOC
{:toc}
---
geekdocCollapseSection: true
---
* TOC
{:toc}

The system is currently envisioned as being in several components.

![POMS Architecture]({{ site.url }}/docs/images/poms_arch.svg)

The dashed lines indicate http protocol connections; the solid line is a database connection;
and the dashed box indicates the boundaries of the system.

The web service will be implemented in Python in CherryPy with a SQLAlchemy database interface; as is
common for many of the SAM related web services.

Although this diagram shows only one SAM-Web and FTS instance; there would certainly be many such
connections in the production system.

### Database

The database will keep track of requested tasks, batch-level tasks (and their associated jobs and input/output datasets),
service avaliabilty, related people, and other associated information.

![prod_mgmt_tasks]({{ site.url }}/docs/images/prod_mgmt_tasks.svg)

### Tasks

The database will keep track of high-level Tasks, as they are requested, approved, and assigned,
and then batch-level tasks will be generated from them, which will then be used to launch jobs
in the batch system. The individual jobs for each task will be associated with the task, and
tracked as they run. Output generated from the task that has been dropped off with an FTS for
filing will be watched and also associated with the batch-level tasks.

### Services

The system will interface with FIFEmon to determine if needed services are available, and keep
availability history information. This is important for two major reasons:

1. The current status of services will indicate if job submission should be held, or be allowed to run.
2. The status of services at the time a given job failed is important for post-mortem analysis of failed jobs -- for example if SAM or DCache were down when the job started; that would explain a failure to transfer input files.


### People

The system needs to know who requests Tasks, who approves them within the experiment, who assigns them in OPOS, etc. This will be Experimenters in the DataDictionary

[Campaign Slicing]({{ site.url }}/docs/internals/technical_design/campaign_slicing)