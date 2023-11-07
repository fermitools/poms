---
layout: page
title: "Client Documentation"
---
* TOC
{:toc}
The following methods are available in the poms_client package. 

This documentation reflects version v4_2_0 of poms_client for v4_2_0 of POMS.

In addition to this, in v4_2_0, most informational page URL's in the regular POMS interactive interface can be called with @&fmt=json@ on the end, to get a JSON dump of the data on the page.

## Current Methods

##### <a style="color:green">campaign_rm</a> (experiment, name, test=False, role=None, configfile=None):

- Remove a campaign.

##### <a style="color:green">campaign_stage_submissions</a> (experiment, role, campaign_name, stage_name, test=None, configfile=None, **kwargs):

Get a JSON dump of submissions to a campaign stage, like:


    {
        "campaign_name": "fake demo v1.0  w/chars",
         "stage_name": "f eg v1.0 - w/chars",
         "campaign_stage_id": "1",
         "data": {
             "tmin": "2019-09-09T15:18:10",
             "tmax": "2019-09-16T15:18:10",
             "nextlink": "/poms/campaign_stage_submissions/samdev/production?campaign_name=fake demo v1.0  w/chars&stage_name=f eg v1.0 - w/chars&campaign_stage_id=1&campaign_id=890&tmax=2019-09-23+15:18:10&tdays=7",
             "prevlink": "/poms/campaign_stage_submissions/samdev/production?campaign_name=fake demo v1.0  w/chars&stage_name=f eg v1.0 - w/chars&campaign_stage_id=1&campaign_id=890&tmax=2019-09-09+15:18:10&tdays=7",
             "tdays": 7.0,
             "tminsec": "1568063890",
             "depends": {
                "1267": null
             },
             "depth": {
                "1267": 0
             },
             "submissions": [
                  {
                    "submission_id": 1267,
                     "jobsub_job_id": "23733516@jobsub02.fnal.gov",
                     "created": "2019-09-10T11:28:29",
                     "creator": "mengel",
                     "status": "Located",
                     "jobsub_cluster": "23733516",
                     "jobsub_schedd": "jobsub02.fnal.gov",
                     "campaign_stage_name": "f eg v1.0 - w/chars",
                     "available_output": 10,
                     "output_dims": "ischildof:(  snapshot_for_project_name mengel-fife_wrap_20190910_112913_3600585  ) and create_date > '2019-09-10 11:28:29' and  file_name like '%root' and version 'v1_2'"
                 }
            ]
        }
    }


##### <a style="color:green">get_campaign_id</a> (experiment, campaign_name, test=None, role=None, configfile=None):

- Lookup a campaign's id from its name.

##### <a style="color:green">get_campaign_list</a> (test_client=False, experiment=None, role=None):

- Get list of campaigns for an experiment.

##### <a style="color:green">get_campaign_name</a> (experiment, campaign_id, test=None, role=None, configfile=None):

- Get a campaigns name given its id.

##### <a style="color:green">get_campaign_stage_id</a> (experiment, campaign_name, campaign_stage_name, test=None, role=None, configfile=None):

- Get campaign stage's id from its name, and its campaign's  name.

##### <a style="color:green">get_campaign_stage_name</a> (experiment, campaign_stage_id, test=None, role=None, configfile=None):

- Get stage name from id.

##### <a style="color:green">get_submission_id_for</a> (campaign_stage_id, user=None, command_executed=None, input_dataset=None, parent_submission_id=None, submission_id=None, test=None, experiment=None, configfile=None):

- Get a submission id for a new submission in a campaign and/or (with submission_id)  update information about the submission.

##### <a style="color:green">job_type_rm</a> (experiment, name, test=False, role=None, configfile=None):

- Remove a job type.  This will only work if nothing is using it...

##### <a style="color:green">launch_campaign_jobs</a> (campaign_id, test=None, test_launch=None, experiment=None, role=None, configfile=None):

- Launch a submission for the first stage in a campaign.

##### <a style="color:green">launch_campaign_stage_jobs</a> (campaign_stage_id, test=None, test_launch=None, experiment=None, role=None, configfile=None):

- Launch a submission for a particular campaign stage.

##### <a style="color:green">login_setup_rm</a> (experiment, name, test=False, role=None, configfile=None):

- Remove a particular login_setup configuration.  This will only work if nothing is using it...

##### <a style="color:green">modify_job_type_recoveries</a> (job_type_id, recoveries, test=None, experiment=None, role=None, configfile=None):

- This lets you modify the recoveries for a given job type; the recoveries is a python object (or json dump of same) just as it comes in a workflow  .ini file dump 
  - i.e  @[["proj_status", [["-Osubmit.memory=", "2000MB"]]]]@  A list of recovery-type, option-override pairs.

##### <a style="color:green">show_campaign_stages</a> (campaign_name=None, test=None, experiment=None, role=None, configfile=None, view_active=None, view_mine=None, view_others=None, view_production=None, update_view=None)

- Get json dump of the campaign stages in a campaign

##### <a style="color:green">show_campaigns</a> (test=None, experiment=None, role = None, configfile=None, view_active=None, view_inactive=None, view_mine=None, view_others=None, view_production=None, update_view=None)

- Json dump data from the show_campaigns page.

##### <a style="color:green">submission_details</a> (experiment, role, submission_id, test=None, configfile=None):

Json dump data from a submission details page.  Has lots of bits in it... 

    {
        "experiment": "uboone",
         "role": "production",
         "tl": [
             {
                "campaign_id": 1087,
                 "experiment": "uboone",
                 "name": "vito_prod_muminus_0-2.0GeV_isotropic_uboone",
                 "active": true,
                 "defaults": null,
                 "creator": 29,
                 "creator_role": "production",
                 "campaign_type": null,
                 "campaign_keywords": null,
                 "tags": null,
                 "stages": [{
                        "campaign_stage_id": 1791,
                         "experiment": "uboone",
                         "name": "reco2",
                         "job_type_id": 783,
                         "creator": 29,
                         "created": "2019-01-09T16:43:10",
                         "updater": 29,
                         "updated": "2019-01-10T11:31:33",
                         "vo_role": "Analysis",
                         "cs_last_split": null,
                         "cs_split_type": "None",
                         "cs_split_dimensions": null,
                         "dataset": "None",
                         "software_version": "v07_07_00",
                         "login_setup_id": 516,
                         "param_overrides": [
                             ["--stage ", "reco2"],
                             ["-Oglobal.sam_dataset=", "%(dataset)s"]
                         ],
                         "test_param_overrides": [
                             ["--stage ", "reco2"],
                             ["-Oglobal.sam_dataset=", "%(dataset)s"]
                         ],
                         "completion_type": "complete",
                         "completion_pct": 95,
                         "hold_experimenter_id": null,
                         "creator_role": "production",
                         "role_held_with": null,
                         "campaign_stage_type": "test",
                         "merge_overrides": false,
                         "output_ancestor_depth": 1,
                         "campaign_id": 1087,
                         "experimenter_creator_obj": {
                            "experimenter_id": 29,
                             "first_name": "Vito",
                             "last_name": "Di Benedetto",
                             "username": "vito",
                             "last_login": "2019-03-08T10:03:42",
                             "session_experiment": "uboone",
                             "session_role": "analysis",
                             "root": false,
                             "exp_expers": null
                         },
                         "experimenter_updater_obj": {
                             "experimenter_id": 29,
                             "first_name": "Vito",
                             "last_name": "Di Benedetto",
                             "username": "vito",
                             "last_login": "2019-03-08T10:03:42",
                             "session_experiment": "uboone",
                             "session_role": "analysis",
                             "root": false,
                             "exp_expers": null
                         },
                         "experimenter_holder_obj": null,
                         "experiment_obj": null,
                         "job_type_obj": null,
                         "login_setup_obj": null,
                         "campaign_obj": {
                            "name": "vito_prod_muminus_0-2.0GeV_isotropic_uboone"
                         },
                         "providers": null,
                         "consumers": null,
                         "consumer_associations": null,
                         "provider_associations": null
                     },
                 ],
                 "experimenter_creator_obj": {
                    "experimenter_id": 29,
                     "first_name": "Vito",
                     "last_name": "Di Benedetto",
                     "username": "vito",
                     "last_login": "2019-03-08T10:03:42",
                     "session_experiment": "uboone",
                     "session_role": "analysis",
                     "root": false,
                     "exp_expers": null
                 }
             },
         ],
         "last_activity": "2019-08-20 18:07:02",
         "msg": "OK",
         "data": {
             "view_active": "view_active",
             "view_inactive": null,
             "view_mine": 4,
             "view_others": 4,
             "view_analysis": null,
             "view_production": "view_production",
             "authorized": [true, true]
         }
    }

##### <a style="color:green">tag_campaigns</a> (tag, cids, experiment, role=None, test_client=False):

- Apply tag to campaigns whose ids are in "cids" which is a comma-separated list. 

##### <a style="color:green">update_stage_param_overrides</a> (experiment, campaign_stage, param_overrides=None, test_param_overrides=None, test=None, role=None, configfile=None):

- Lets you set the param overrides on a campaign stage.  These again are a list of pairs of strings, as in the .ini file dump;or a json dump of same.

##### <a style="color:green">update_submission</a> (submission_id, jobsub_job_id=None, status=None, project=None, test=None, experiment=None, role=None, configfile=None):

- Update a submission. 

##### <a style="color:green">uploaded_files_rm</a> (experiment, filename, test=None, role=None, configfile=None):

- Remove an uploaded file.

##### <a style="color:green">upload_file</a> (file_name, test=None, experiment=None, role=None, configfile=None):

- Upload a file.

##### <a style="color:green">upload_wf</a> (file_name, test=None, experiment=None, configfile=None, replace=False, role=None):

- Upload a campaign .ini file.

## Deprecated Methods

##### <a style="color:green">update_session_experiment</a> (experiment, test_client=False):

    You can now pass experiment and role on all calls.  This will still
    set defaults for them if you don't pass them in.

##### <a style="color:green">update_session_role</a> (role, test_client=False):

    You can now pass experiment and role on all calls.  This will still
    set defaults for them if you don't pass them in.

##### <a style="color:green">campaign_stage_edit</a> (action, campaign_id, ae_stage_name, pc_username, experiment, vo_role, dataset, ae_active, ae_split_type, ae_software_version, ae_completion_type, ae_completion_pct, ae_param_overrides, ae_depends, ae_launch_name, ae_campaign_definition, ae_test_param_overrides, test_client=None, configfile=None) experiment=None, role=None)

##### <a style="color:green">campaign_definition_edit</a> (output_file_patterns, launch_script, def_parameter=None, pc_username=None, action=None, name=None, experiment=None, recoveries=None, test_client=False, configfile=None)
##### <a style="color:green">campaign_edit</a> (**kwargs):

##### <a style="color:green">register_poms_campaign</a> (campaign_name, user=None, experiment=None, version=None, dataset=None, campaign_definition=None, test=None, role=None, configfile=None):

##### <a style="color:green">job_type_edit</a> (output_file_patterns, launch_script, def_parameter=None, pc_username=None,

##### <a style="color:green">launch_template_edit</a> (action=None, launch_name=None, launch_host=None, user_account=None, launch_setup=None, experiment=None, pc_username=None, test_client=False, role=None, configfile=None)

##### <a style="color:green">get_task_id_for</a> (campaign, user=None, command_executed=None, input_dataset=None, parent_task_id=None, task_id=None, test=None, experiment=None, role=None, configfile=None):

- This is the old name for get_submission_id_for().

##### <a style="color:green">launch_jobs</a> (campaign, test=None, experiment=None, role=None, configfile=None):

- This is the old call for launch_campaign_stage_jobs 
