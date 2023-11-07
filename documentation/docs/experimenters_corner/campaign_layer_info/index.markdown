---
layout: page
title: Campaign Layer Info
---
* TOC
{:toc}
To get the full features of POMS, when creating a CampaignLayer, you should have the following information and provide it to POMS, either in the CampaignLayerDefinition or in the CampaignLayer:

* Name for the general type of CampaignLayer for the definition, and for this specific instance (i.e. MyExperimentMonteCarlo and MyExperimentMonteCarlo_v4_3_purple_events)
* Job launch details
  * Software launch script that you use
  * what VO Role to use (i.e. Analysis, Production, etc.)
  * Account/host you wan the launches run from
* parameters to software launch script to set
  * SAM dataset
  * software version, that will end up in SAM metadata as application version
* Dataset(s) describing all files this CampaignLayer will process
* How you want the dataset sliced into pieces for submissions; currently supported options:
  * **new** -- files added since last launch
  * **mod_n** -- (where n is an integer) sliced into n chunks.
  * **draining** -- don't slice it, the dataset is a draining dataset which already excludes already processed files
* list of types of files the CampaignLayer generates, (i.e. if your program generates a processed data file and a histogram, how do we tell them apart?)
* specify how many recoveries you want run, and what sort of recovery dataset you want to build on each recovery
* if this CampaignLayer depends on the output of another campaignLayer, which one, and what file patterns.
* if the jobs have a specific number of input/output files (i.e. it reads only one file and writes only one) what those numbers are.

### Future releases

In future, we may also want to know

* memory/disk requirements
* expected job runtime

and we may have the option to increase memory usage requested for recovery jobs.