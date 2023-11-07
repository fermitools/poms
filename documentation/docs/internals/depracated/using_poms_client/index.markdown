---
layout: page
title: Depracated/using Poms Client
---
* TOC
{:toc}
Using the poms_client package to have your jobs tracked in POMS. This involves creating/updating a CampaignLayer for this group of submissions, and a Task or Submission for this particular job submission, and passing the id's for those into the job so that the monitoring will associate them with the jobs.

### setup

You can find the tools by doing:

    setup poms_client

or perhaps

    setup poms_client -z /grid/fermiapp/products/common/db

    setup poms_client -z /cvmfs/fermilab.opensciencegrid.org/products/common/db

### python usage

In a python script, you should:

import poms_client 

    ....
        campaign_id = poms_client.register_poms_campaign(
            campaign_name, 
            experiment = "your_experiment", 
            version = "your sam application version", 
            role = "Production",
            dataset = "optional overall dataset for this campaign",
            campaign_definition = "optional campaign definition name" 
        )

        task_id = poms_client.get_task_id_for(
            campaign_id, 
            command_executed="commands string for job triage",
        )


### bash usage

In a shell script you would similarly use the command line tool:

    campaign_id=`register_poms_campaign --campaign='campaign_name' --experiment='your experiment' version=...`

    task_id=`get_task_id_for --campaign=$campaign_id --experiment='your experiment' --command_executed=...`

Note that technically you only need to register the campaign when it is new or if any of the
information changes; and you could just use the campaign id or campaign name when caling
get_task_id_for, but it's often easier to always call register_poms_campaign and then it will be
updated if, for example, the software version changes.

    launch_template_edit \
    --action=add \
    --experiment=samdev \
    --launch_name some_name  \
    --host bel-kwinith.fnal.gov \
    --launch_account samdevpro
    --setup "source /grid/fermiapp/products/etc/setups.sh; setup fife_utils"  

    job_type_edit \
        --action=add \
        --experiment=samdev \
        --name=some_type_name \
        --launch_script="fife_launch -c silly.cfg -O global.dataset=%(dataset)s" 
        --def_parameter="['name', 'value']" 

    campaign_edit \
        --action=add \
        --experiment samdev \
        --campaign_name mwm_tstc_0911_01 \
        --dataset gen_cfg \
        --launch_name samdev-o-rama \
        --job_type fakeeventgen \
        --vo_role Analysis \
        --completion_type located


### Passing the job and campaign id's to jobsub_submit or jobsub_submit_dag

Then after getting task and campaign id's, you should pass them into jobsub in a couple of ways, by adding extra arguments to your jobsub_submit command line:

* In the environment, with

      -e POMS_CAMPAIGN_ID=$campaign_id -e POMS_TASK_ID=$task_id

* As their own classad with

      -l "+POMS_TASK_ID=$task_id" -l "+POMS_CAMPAIGN_ID=$campaign_id" 

* And in a combined tag, with

             
      -l "+FIFE_CATEGORIES='\"POMS_TASK_ID_$taskid,POMS_CAMPAIGN_ID_$campaign_id,${othertags}\"'" 


* Note that for jobsub_submit_dag, on that FIFE_CATEGORIES item, you need triple backslashes, rather than single ones, to get it passed through.

Note that in many cases, its easier to use the "poms_jobsub_wrapper" version of jobsub_submit, and jobsub_submit_dag which will do all of this for you, but as it has a somewhat fragile setup (it depends on being in the PATH in front jobsub_client) building these options into a script is usually more reliable.