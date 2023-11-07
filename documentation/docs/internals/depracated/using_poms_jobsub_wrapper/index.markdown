---
layout: page
title: Depracated/using Poms Jobsub Wrapper
---
* TOC
{:toc}
To use poms_jobsub_wrapper, you have to first create a campaign by hand in the POMS GUI, (or use
poms_client (just the campaign part of [Using Poms Client]({{ site.url }}/docs/internals/depracated/using_poms_client)) see:

* Campaign Definition Edit Help
* Launch Template Edit Help
* Campaign Edit Help

And then get the numeric campaign_id by looking int he URL of the campaign info page.

Then you can:

    setup jobsub_client
    setup poms_jobsub_wrapper
    export POMS_CAMPAIGN_ID=campaign_id

    jobsub_submit -<usual jobsub options>

Note that you have to setup poms_jobsub_wrapper **after** you setup jobsub_client, otherwise
we won't find the wrapper when we run jobsub_submit.