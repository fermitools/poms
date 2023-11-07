---
layout: page
title: Getting More Memory In Recovery Jobs
---
* TOC
{:toc}
Two approaches here:

* For jobs with fixed/no inputs (i.e. event generation) use [autorelease](https://cdcvs.fnal.gov/redmine/projects/fife_utils/wiki/Using_autorelease_for_more_memory) to restart jobs that get held for memory.
* For jobs which read SAM datasets, use recovery launches


#### Making jobs not go Held forever for Memory

Note that to make this work smoothly, you need your jobs that go over memory to not hang around in "Held" status  
forever. You can avoid this by setting:

    [stage_whatever]
    ...
    submit.line_1 = +PeriodicRemove=JobStatus==5&&HoldReasonCode==26&&CurrentTime-EnteredCurrentStatus>3600


in your fife_launch config, or by adding

    --line '+PeriodicRemove=JobStatus==5&&HoldReasonCode==26&&CurrentTime-EnteredCurrentStatus>3600'


to your jobsub_submit parameters otherwise.


#### Adding recoevery launches

You can, in your JobTypes, add recovery launches, and in particular you can add ones that override launch options to request more memory. If you are using fife_launch, this can be accomplished by

* Opening the campaign in the GUI Campaign editor

![a_r_s1.png]({{ site.url }}/docs/images/a_r_s1.png)

* double clicking on the job type
![a_r_s2.png]({{ site.url }}/docs/images/a_r_s2.png)

* change the name (maybe add _with_mem_recovery)
* click the Edit button next to Recoveries

![a_r_s4.png]({{ site.url }}/docs/images/a_r_s4.png)

* pick proj_status for the recovery type
* click the edit button on the right to edit the Param Overrides
* in the param editor, set the override for submit.memory for fife_launch

![a_r_s5.png]({{ site.url }}/docs/images/a_r_s5.png)

* Accept/OK in each popup

![a_r_s6.png]({{ site.url }}/docs/images/a_r_s6.png)
![a_r_s7.png]({{ site.url }}/docs/images/a_r_s7.png)

* check stages that use that jobtype to get the new one, and press Done

![a_r_s8.png]({{ site.url }}/docs/images/a_r_s8.png)

* press Save for the whole campaign.
