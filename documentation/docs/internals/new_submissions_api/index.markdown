---
layout: page
title: New Submissions Api
---
* TOC
{:toc}
So rather than scrape condor_q output going forward, we're going to talk to the new submissionsAPI provided by the good folks doing Landscape.

It has an interactive front end:

[View Landscape](https://landscape.fnal.gov)

[View Landscape's GraphQL interface](https://landscape.fnal.gov/lens)

And we're calling it from [submission_broker/submission_agent.py](https://github.com/fermitools/poms/blob/master/submission_broker/submission_agent.py)