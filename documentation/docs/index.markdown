---
layout: page
title: POMS Overview
kramdown:
  gfm_quirks: [paragraph_end]
author: "Marc Mengel"
---

* TOC
{:toc}


The *Production Operations Management System* (POMS) is a project designed to provide  a service to assist production teams and analysis groups of experiments in their MC production and DATA processing. As the quantity of data originated by the running experiments greatly increases, the ability of simplifying the steps in data processing and management has become more and more appealing to the users.

POMS provides a web service interface that enables automated jobs submission on distributed resources according to customers’ requests and subsequent monitoring and recovery of failed submissions, debugging and record keeping.
The ultimate goal is the most efficient utilization of all computing resources available to experiments, while providing a simple and transparent interface between users and the complexity of the grid.

POMS runs behind a web service interface that provides both interactive pages to the users, and a REST interfaces to scripts that interact with it. This means that experiments can use POMS through their web browser to configure and run their production code, or they can use the poms_client and poms_jobsub_wrapper tools to submit jobs through a command line and have POMS tracking, debugging and monitoring them.

---




## [POMS User Documentation]({{ site.url }}/docs/user_documentation)

For the full documentation please refer to

* [POMS User Documentation]({{ site.url }}/docs/user_documentation)
* [Campaign Editor]({{ site.url }}/docs/gui_workflow_editor_user_guide)
* [POMS Client Documentation]({{ site.url }}/docs/client_documentation)
* [Release Notes]({{ site.url }}/docs/release_notes)
* [Release v4.0.0 Backward Compatibility - What You need to know]({{ site.url }}/docs/backwards_compatibility)

POMS is interfaced to the following systems:

* ["Jobsub"](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki): a service that provides support for the job lifecycle enabling the management of jobs on distributed resources, such as the Grid.
* ["SAM"](https://cdcvs.fnal.gov/redmine/projects/sam/wiki/User_Guide_for_SAM): the data handling system, to keep track of files, their meta-data and processing. 
* ["FIFEmon"](https://landscape.fnal.gov/monitor): for monitoring.
* ["FIFE_UTILS"](https://cdcvs.fnal.gov/redmine/projects/fife_utils/wiki)

----

The production instance of POMS is at: [https://pomsgpvm01.fnal.gov](https://pomsgpvm01.fnal.gov/)

If new to computing at Fermilab, please read: FIFE Wiki page: ["Welcome New Computing Users"](https://cdcvs.fnal.gov/redmine/projects/fife/wiki/Welcome_New_Computing_Users)

Users' mailing list : poms_announce@fnal.gov

---

{% unless page.is_github %}
## [POMS Internals]({{ site.url }}/docs/internals)

* This is only intended for developer audience
{% endunless %}

## [Frequently Asked Questions]({{ site.url }}/docs/frequently_asked_questions)

<br>

----
For anything not covered within this documentation, please feel free to reach out to the [POMS Admin](mailto:ltrestka@fnal.gov)