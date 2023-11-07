---
layout: page
title: Harnessing Data Dispatcher with POMS - Your Campaign Command Center
---
* TOC
{:toc}

## Introducing Data Dispatcher for POMS!
* Data Dispatcher is one of three new systems in place that collaboratively manage the storage/location of files and metadata, and orchestrates the execution of projects based on a given project id, dataset, or query.
    
* In our setup, [Data Dispatcher](https://data-dispatcher.readthedocs.io/en/latest/) leverages [Metacat](https://metacat.readthedocs.io/en/latest/webapi.html) for metadata cataloging, and [Rucio](https://rucio.github.io/documentation/) for file lookup.
    
* POMS is now adept at managing campaigns and their subsequent stages utilizing [Data Dispatcher](https://data-dispatcher.readthedocs.io/en/latest/), shifting away from the SAM platform.
    
* Our implementation aims to maintain consistency with the SAM utilization approach, albeit with a few distinctions which are elaborated upon in the sections below.
    
&nbsp;

## Why Transition to Data Dispatcher from SAM?

* SAM, being an older platform, necessitates numerous steps to ascertain the status of a project/definition and lacks diversity in achievable functionalities.

* Data Dispatcher empowers experimenters to monitor job updates in real-time without imposing a significant load on the existing system. Moreover, POMS furnishes a user interface displaying crucial details of all submissions made through it.

* Projects in Data Dispatcher can be swiftly restarted, duplicated, or altered without adhering to a strict naming convention. Attributes can be appended to a project to provide local information like campaign, stage, submission, or split type used, which is easily accessible and can be queried if necessary.

## Prerequisites

* Ensure Rucio is set up for your experiment to define the physical location of files.
* Experimenters need to have accounts on Metacat:
	* Provide the[POMS Admin](mailto:ltrestka@fnal.gov) with the Metacat server URL and auth server URL, which will be utilized by POMS.
	* Your Metacat server should either have an account with admin privileges for POMS, or you should create a group for POMS which every experimenter should be a part of (can be segregated based on analysis/production roles).
	* For transitioning to Data Dispatcher on POMS, reach out to the  [POMS Admin](mailto:ltrestka@fnal.gov) for onboarding your experiment/group.

## Instructions for Setting Up and Using Data Dispatcher on POMS:
 * [Environment Setup]({{ site.url }}/docs/experimenters_corner/data_dispatcher/environment_setup/index.html)
 * [Poms Campaign Setup]({{ site.url }}/docs/experimenters_corner/data_dispatcher/poms_campaign_setup/index.html)
 * [Split Types and Data Dispatcher Fields]({{ site.url }}/docs/experimenters_corner/data_dispatcher/split_types_and_data_dispatcher_fields/index.html)
 * [Tracking Submissions]({{ site.url }}/docs/experimenters_corner/data_dispatcher/tracking_submissions/index.html)