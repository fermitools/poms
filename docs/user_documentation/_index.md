---
title: "User Documentation"
geekdocToC: 6
---

{{< expand "Table of Contents" >}}
{{% toc %}}
{{< /expand >}}



### POMS Overview

The *Production Operations Management System* (POMS) is a project designed to provide  a service to assist production teams and analysis groups of experiments in their MC production and DATA processing. As the quantity of data originated by the running experiments greatly increases, the ability of simplifying the steps in data processing and management has become more and more appealing to the users.

POMS provides a web service interface that enables automated jobs submission on distributed resources according to customers’ requests and subsequent monitoring and recovery of failed submissions, debugging and record keeping.

POMS is interfaced to the following systems:

* [Jobsub](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki): a service that provides support for the job lifecycle enabling the management of jobs on distributed resources, such as the Grid.

* [SAM](https://cdcvs.fnal.gov/redmine/projects/sam/wiki/User_Guide_for_SAM): the data handling system, to keep track of files, their meta-data and processing. 

* [FIFEmon](https://landscape.fnal.gov/monitor): for monitoring.

* [FIFE_UTILS](https://cdcvs.fnal.gov/redmine/projects/fife_utils/wiki)

* ECL, the Electronic Collaboration Logbook  where experiments can keep track of the production and processing operations as a collection of records chronologically organized in the form of "logbook entries".

The ultimate goal is the most efficient utilization of all computing resources available to experiments, while providing a simple and transparent interface between users and the complexity of the grid.

POMS runs behind a web service interface that provides both interactive pages to the users, and a REST interfaces to scripts that interact with it. This means that experiments can use POMS through their web browser to configure and run their production code, or they can use the poms_client and poms_jobsub_wrapper tools to submit jobs through a command line and have POMS tracking, debugging and monitoring them.

To help understanding some terminology used in this document a [Glossary]({{< ref "/glossary" >}})  is provided.

#### Basic Concepts

POMS employs the concept of a “Campaign” to achieve the goal of data processing.  A “Campaign” is a collection of one or more stages executed in a user defined order to achieve the goal of data processing.  

Each campaign stage configuration involves three main steps, mainly the definition of parameters  and actions which will be used to launch the jobs: 
   
*  Compose a login/setup.

*  Compose the job type.

*  Compose one or more campaign stages providing the login/setup template and job  types previously defined.

*Note:* _Both the Login/Setup and Job Type definitions can and should be reused._

#### Getting access: New Experiments, New Accounts, Users and Roles

To be able to use POMS interface the user needs to have an account. As this account has a level of authority in the experiment, its creation must be approved by the experiment.  The user needs to do the following to request an account (We assume the user has already a Fermilab Service account).
* Open the webpage to Service Now[(SNOW)](https://fermi.servicenowservices.com/wp) and login using the Service account.
* Select the tile "Request Something".
* Select the tile "Experiment/Project/Collaboration Computing Account".
* Choose your experiment.
* Choose the role "Production" or "Analysis".   Analysis can view production campaign data, but cannot yet create its own.
 
It can take up to one hour from when your request is approved until it appears in POMS.

When a new experiment is interested in using POMS, a Service Desk ticket needs to be opened with the request. The user can go the Service Desk Website
[Scientific Computing Services](https://fermi.servicenowservices.com/nav_to.do?uri=%2Flist_service_areas.do%3Fdivision%3Dscientific_computing) , select [Scientific Production Processing](https://fermi.servicenowservices.com/cmdb_ci_service.do?sys_id=bc926c686f46420032544d1fde3ee498&sysparm_view=ess&sysparm_affiliation=) and under the 'Get Help' select 'Submit a request to service providers'.

POMS has a concept of user _Roles_ to control various operations on the components. User get assigned role also outside POMS through FERRY. Depending on the role, a user can perform certain actions. 

Three roles are provided : superuser, production and analysis.

* The highest role is  _superuser_ . This used to be _coordinator_  but has been changed to _superuser_ to comply  with FERRY. To acquire the super user role go to the SNOW page "Add/Remove JobSub Group Superuser of an [Experiment](https://fermi.servicenowservices.com/wp?id=evg-search-results&sysparm_filters=search_string%3Djobsub%20superuser). When you click on the link you will have to follow and submit a form with your information. A user with this role can modify any jobs.
* Those with _production_ role are able to modify ALL production templates.
* Those with _analysis_ role can only modify their templates. 

When logging in POMS, for the first time, the user has, by default, the lowest role allowed for the account/experiment.

#### A  _"Practical Scenario"_

Let's envision a very possible scenario for an experiment during the initial phase of simulation:

We want to use the production processing to simulate and recostruct events in a portion of the detector to test reconstruction accuracy.   Most likely we would use a MonteCarlo type programs to generate particles events in a detector, store data in files  which in turn will be used to reconstruct the events and then finally analyze the data.

From POMS point of view, the overall process will represent a 'campaign' with possibly five stages:

Stage 1: Event Generation jobs.  
Stage 2: Geant simulation jobs.  
Stage 3: Detector Simulation jobs.  
Stage 4: Reconstrution jobs.  
Stage 5: Analyzing jobs.  

Basic information need to be provided for each stage to achieve the goals of the whole campaign which could be simplified as the _"who-where-how"_:
* _who_
  * the account to use to login into the host (unix account)
* _where_
  * the host on which the login script will be launched 
* _how_
  * the script to use to configure the environment
  * the job types to be used for each stage
  * the scripts to be used when submitting the jobs and the parameters to be used
  * the dependency between stages
  * the criterion to determine when a stage is considered finished so we can move on to the next stage

Those steps are specified and grouped in three type of templates: _"Login Template"_, _"Job Types"_, _"Campaign stages"_ which will be first viewed in the next section before expounding  in the details .


#### The _"Big Picture"_

Before entering into the details of the components, let's examine the following _"Big Picture"_ for a hypothetical campaign called eve_mc which could represent the above 'Practical Scenario'.  
This representation is done using the Campaign Editor, a POMS built-in tool which allows to create and manipulate campaigns. Details on how to use the tool  are available in the [Campaign Editor User Guide]({{< ref "/gui_workflow_editor_user_guide" >}}) User Guide.

![eve_mc_campaign.png](/docs/images/eve_mc_campaign.png)

Let's see first a brief description of the elements in the campaign's layout:

* _Login/Setup_ : This is the basic component which defines the host from which jobs will be launched, the account used to login into the host and the environment POMS configures for the jobs that will be launched. If you double click on the 'generic_fife_launch' login/setup element you will see the following:

![eve_mc_login.png](/docs/images/eve_mc_login.png)

* _Job Type_ : This defines the type of job used for processing. Strictly speaking, there is no 'field' to store the job type, instead,  the type is the result of the launch script and parameters which are used to accomplish the purpose of the job.  As a suggestion, the user could give  it a meaningful name to reflect its purpose, for example _'Myjob_MC_' for a MonteCarlo job.  
If you double click on the 'generic_fife_process' login/setup element you will see the following:

![eve_mc_jobtype.png](/docs/images/eve_mc_jobtype.png)

* _Campaign Stage_: A stage consists of a Login/Setup, a Job Type and a set of definitions and parameters used to run the jobs for accomplishing a 'stage' of the whole campaign. Campaign stages can be connected by dependencies, for examples, files produced by one stage can be used as input for the next stage.  
If you double click on a stage element you will see the following:


![eve_mc_stage.png](/docs/images/eve_mc_stage.png)

* _Campaign Stage Dependency_: It defines how the stages depend on each other, typically specifying the file pattern of the files produce by one stage and used as input by the next stage.  
If you double click on the arrow between two stages you will see the following:

![eve_mc_dependency.png](/docs/images/eve_mc_dependency.png)

* _Campaign Defaults_:  default values used when adding a new stage. If you double click on the arrow between two stages you will see the following:

![eve_mc_defaults.png](/docs/images/eve_mc_defaults.png)


Campaign stages can branch out, have further dependency and eventually come back together. An example of a possible scenario based on the previous example could be if after generating the events, the user wants to continue the simulation under two different detectors configurations, with and without noise , and then compare the results.  
The picture below shows this new scenario:

![eve_mc_campaign_double.png](/docs/images/eve_mc_campaign_double.png)



Now let's see the components in more details. The basic components can be created following the links in the Main Menu; _Sample Campaigns_ are also available to provide samples of campaigns which can be cloned and then customized using the _Campaign Editor_.

#### Drilling down

Campaign definition, as already mentioned,  involves the definition of a _login/setup_ , a _job type_  and one or more _stages_.  
We strongly reccomend to use the Campaign Editor to define the  campaign stages, however, they can all be built using the links from the main menu.  
At the time of writing, creating a new login setup and a job type can ONLY be done using the links from the main menu and that is shown in the following sections.


#### Compose a login/setup script

Campaign setup involves the definition of a _login/setup_ which is a collection of bash commands to be executed on the experiment’s user defined host to establish the environment from which POMS will launch all jobsub jobs.

As a user may belong to more than one experiment, as a first step, make sure you select the experiment and role from the pull-down menu on the top right corner. 

Templates can be reused, cloned  and modified for different campaigns and are available to other users as well. So you can use the same template for different processing campaigns. 

Existing templates could be viewed following the  'Compose Login/setup'  link on the main page side panel to see if there is already a template usable for the purpose.  To create a new template just click the 'Add' button at the bottom of the page.

![login_example.png](/docs/images/login_example.png)

Four fields need to be added or modified depending on the action:

* _Name_: Define the name of your login template. When you _clone_ the login setup, the name will show with the prefix 'CLONE OF:current name'

* _Host_: Define the interactive node or machine you are going to use to launch the login/setup script.

* _Account_: Define the user login account for the launch (ex.  novapro, minervapro, minervacal, uboonepro, etc )

* _Setup_: Define the environment variable, setups, or scripts that you usually setup on your own machine (e.g. setup_nova, launch script)  if you were to launch the jobs from the shell command line.
**This must be done in one semicolon separated shell line, one line only, no <CR> otherwise the script will break.**  
Typically, as part of the setup, [FIFE_UTILS](https://cdcvs.fnal.gov/redmine/projects/fife_utils/wiki) and [JOBSUB](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki#Jobsub-services-on-fifebatch-infrastructure) are used. 


Note: The Account above will need the following .k5login entries:

       poms/cd/fermicloud045.fnal.gov@FNAL.GOV
       poms/cd/pomsgpvm01.fnal.gov@FNAL.GOV
       <YOUR KERBEROS USERNAME>@FNAL.GOV


You MUST add your default principal to the .k5login file if the .k5login file exists.  So if you're creating a .k5login file for the first time don't forget to add your account name in addition to the poms ids.  Your Fermilab username must be lower case and the FNAL.GOV must be uppercase.

#### Compose a job type
   
Next step is to compose the job type. A Job 'Type' is a way of categorizing the job based on its purpose, for example, Monte Carlo, Calibration, Reconstruction etc. Jobs of the same 'type' will typically have same or similar set of parameters.  
Since the Job Type defines the purpose of the processing accomplished in a certain stage, you can have different job types for the different campaign stages.  
Furthermore, a typical way stages are connected is through the use of files produced by the previous stage and before the next stage can be started, you need a way to let the work flow know that the previous stage is finished.  This is also done configuring the Job Type.

You can view, create or clone existing job types following the main menu link to 'Compose Job Type'.

The page might show existing job types; as per the template you can modify existing ones or click the ‘Add’ button to create a new one.


![job_type_example.png](/docs/images/job_type_example.png)

The following are the fields that need to be filled:

* _Name_: A name that describes the kind of job campaign you are running (eg. Nova raw2root keepup FD, rock_neutrinoMC, minerva_cal).

* _Output file patterns_: The output pattern you are interested in your campaign (eg. %.root)

* _Launch Script_: In this field, you need to put the script that you run to submit jobs in your machine. In the example above, we specify the config file where variable definitions are specified to be used by fife_launch. Example of a config file can be found clicking on the 'Config File Templates' link from the main menu.

* _Definition Parameters_: The arguments your launch script (included in Launch script) used for the submission. In the example above, we specify to submit 5 jobs.

* _Recovery Launches_: When jobs complete there might be errors so that you want to re-launch the campaign stage: a Recovery Launch field is where you specify options to re-submit jobs based on their failure.  Example of available options:
  * added_files: include files added to definition since previous job ran.
  * consumed_status: include files which were not flagged "consumed" by the original job. In the example above, we specify to re-submit starting 3 jobs.
  * delivered_not_consumed: include only delivered files which were not "consumed" by the original job.
  * pending_files: include files which do not have suitable children declared for this version of software.
  * process_status: like consumed status, but also include files which were processed by jobs that say they failed.


About the use of the _'Output file patterns'_:  
User's Jobs can use input files and produce output files  (it is responsability of the user's job to declare the files to SAM for further data handling).  
From the POMS perspective, when configuring the JOB type, the user can specify the output file patterns of the files produced by the jobs; these will be then used by POMS to check on the 'completion' level of the campaign stage the jobs ran for.

About the use of the _'Launch Script'_: 
The launch script is used when starting the job using, in typical case fife_launch (example [mvi: change accordingly when showing the real example..]):  
> fife_launch -c /sbnd/app/users/dbrailsf/poms/soft/srcs/sbndutil/cfg/poms/sbnd_launch.cfg   

POMS strongly suggests the use of [fife_launch](https://cdcvs.fnal.gov/redmine/projects/fife_utils/wiki/Fife_launchfife_wrap) which is a config-file based job launcher script; fife_launch is the front-end to jobsub_submit which is part of the [Jobsub](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki/Using_the_Client) client library which in turn does the final job submission.  
Example of a config file can be found clicking on the 'Config File Templates' link from the main menu.

#### Compose a campaign stage

A stage belongs to a campaign so the only way to create a stage, since POMS version > 4.X.X,  is to use the Campaign Editor.  
In previous versions a form was provided when selecting  the 'Compose Campaign Stages' menu item under 'Configure Work' but this has been discontinued.  
For a detailed description of the fields and how to create,edit etc a stage please refer to the [Campaign Stages]({{< ref "#a-stylecolorgreencampaign-stagesa" >}}) section.
        
#### Bringing it all together

Using the _Compose Campaign Stage_ from the main menu implies filling up the forms for each stage to build the whole campaign. 
This is where the _Campaign Editor_ becomes very useful so you can create all the stages and define how the stages depend on each other.  
Let's see how we can get to the campaign eve_mc used in the initial example using the Campaign Editor.  
Let's assume we have already defined the Login/setup and the job type. Also, let's assume we have created one stage for it, so basically starting with the following picture:

![eve_mc_one_stage.png](/docs/images/eve_mc_one_stage.png)

As you can see in picture below, if you right click on the existing stage a pop up window will show up where you can 'Add Node' which is the editor generic notation for 'Add stage' in our case; replace ' undefined' with the stage name you want, in our case eve_g4: click OK and you will end up with what it is shown in the picture on the right.

![eve_mc_add_stage.png](/docs/images/eve_mc_add_stage.png)  ![eve_mc_two_stages.png](/docs/images/eve_mc_two_stages.png)

The second stage has been created with the default values and you can then open the form and change the fields accordingly.  
The arrow that connects the two stages represents the dependency from the first stage. Double clicking on the arrow it shows the type of dependency, in this case stage eve_g4 will use files with pattern 'root' created by eve_gen.  
If we  continue to add stages in the same fashion  the whole campaign will be generated.

![eve_mc_dep_1_2.png](/docs/images/eve_mc_dep_1_2.png)


#### Running the Campaign.

Now that the campaign has been defined, we can launch jobs, and to do so,  under the main menu, section  'Campaign Data’ click on  Campaign. This will show all the existing campaigns.    
You can filter on the names to find your campaign and then click on the 'Launch' symbol to start it;  this will start it from the first stage.  

Find more information in the [Campaigns]({{< ref "#campaigns" >}}) section below.

### POMS: Navigation Overview

Lets see a general description of POMS from the Navigation perspective.  
Logging into POMS will direct you to the Home Page:

![poms_home_page.png](/docs/images/poms_home_page.png)

On the top right corner two selector fields show the experiment and the role based on your account; if you belong to multiple experiments you will be able to select accordingly.

The Main Menu on the left panel is organized in various sections which allow the user to view, configure and monitor the work:

* External Links:
  * Logbook: Link to the Electronic Collaboration Logbook for the experiment if available.
  * POMS SNOW Page: Link to POMS Service Desk Page (Service information)
  * Downtime Calendar: Link to the Scientific Services Outage Calendar

* Experiment:
This page shows the members of the current group/experiment. Data is presented as a table which lets you sort and filter the results to easily find people:
  * You can click on column headings to sort the data.
  * You can type substrings in the boxes under the column headings to filter for matches.

* Campaign Data:
  * Campaigns: link to the list of existing campaigns for the experiment.
  * Campaign Stages: link to the list of existing stages.
  * Sample Campaigns: link to the list of existing samples to be used as templates when creating new campaign.

* Configure works:
  * Compose Login/setup: link to the list of existing login templates and possible actions.
  * Compose Job Type: link to the list of existing job types  and possible actions.
  * Compose Campaign Stages: link to the list of existing stages  and possible actions.
  * Config File Templates: link to some useful templates that can be used when launching jobs.

* Jobs:
These links direct to _Landscape_ plots to monitor campaigns and jobs status. 

From the center tiles selection 

### POMS: Detailing the components

### Campaigns

One of the most useful section from the Menu is the 'Campaign Data'. Under 'Campaigns'  you can view  all the campaigns and select various actions on them.  
In the following pictures, a filter on the campaign name has been applied to narrow down the list.

![campaigns.png](/docs/images/campaigns.png)

Results can be sorted  by clicking on column headings and can be  filtered by typing strings to match in the boxes under the column headings.  
You can configure what is displayed on the page from the group of check boxes. By default, you will see Active campaigns that belong to you and others  with Production role.

* You can perform some actions on the campaigns using the green buttons: 
  * Using the 'Select' check box you can pick which campaign(s) to modify to change  the active/inactive status or assign (or remove) a tag.
  * Click on 'Add' button to add a new campaign.
  * Click on 'Pick .ini file'  to select an existing ini file and then Upload it and in so doing create a new campaign based on the information in the ini file.
* Clicking on the campaign name you will be redirected to the campaign stages page.
* Clicking on 'Submissions running' shows how many submissions are current.
* Clicking on 'Submission History' you will see the submissions for the current week and you can navigate back and forth in time.
* Clicking on 'Dependencies' it shows all the stages for the campaign.
* Clicking on 'GUI Editor' you will be redirected to the Editor page where you can update all the components of the campaign.
* Clicking on  .ini File you will be presented with a page with the list of all the campaign components and dependencies; this file can be saved and modified for further use. For example, it can be updated and then uploaded back into POMS using poms_client API. 
* Tags would show tags assigned for the campaign. People use tags to group campaigns by 'theme'. Tags can be useful when filtering to narrow down the list.
* You can 'Rename' and 'Delete' a campaign.

Of the several actions that can be performed on the campaigns the following:
* Add a campaign
* Cloning the campaign
* GUI editor

Will direct you to the use of the Campaign Editor.

### Campaigns Actions

This section looks a little more in details on the actions you can perform from the Campaigns page.

* Add a campaign.
* Clone an existing campaign.
* Launch the campaign.
* Pause the campaign.
* Resume the campaign.
* View results.
* Re-launch and kill it.

#### Add a campaign

When you choose _Add a campaign_, after being prompted for a name, in this case _eve_calib_ was given, you will be presented with the following page:

![add_campaign_eve_calib.png](/docs/images/add_campaign_eve_calib.png)

A basic campaign _skeleton_ is pre-built with default values, one generic stage which uses the generic login setup and job type.  This example will be used in the [Campaign Editor]({{< ref "/campaign_editor" >}}) documentation to show how to use the tool.



#### Clone an existing campaign

For this purpose we will go to the Campaigns page and clone the Campaign eve_demo which has 3 stages and  different job types for each stage.  
You will be prompted for a new name, in our case eve_test, then you will be redirected to the Campaign Editor Page.

The following picture illustrates the original campaign and the cloned one as they appear in the campaign editor.

![eve_demo_clone.png](/docs/images/eve_demo_clone.png)

As you can see, the new campaign has the new name assigned but all the stages have the _same_ names as the original, HOWEVER once saved, they become private to the new campaign.  
In the bottom section the Login setup and job types are *NOT* cloned. This is purposely done since multiple campaigns could use same job types and users are encouraged to re-use them.   
However, if you need to change some field in the job type, then you must give a new name so that another job type will be created for your campaign so that nothing gets overridden in the original.
For a visual quick association, each stage color is the same as the associated job type.

The following picture shows how to change the name of the cloned stage to be more appropriate to the campaign:

![eve_test_change_stname.png](/docs/images/eve_test_change_stname.png)
The following picture shows the final campaign after changing the stage names.

![eve_test_final.png](/docs/images/eve_test_final.png)

#### Launch the campaign

Now that the campaign has been created, you can launch it going to the 'Campaigns' page , find the campaign and click on the 'Launch' button. 

This will launch the first stage,  eve_test_gen_v1.

The 'Submissions running' will show that the campaign has submission. Then you can check also on the 'Submissions History' page:

![eve_test_submission.png](/docs/images/eve_test_submission.png)

As previously stated, each stage has its own page and can be individually launched which can be good in case of failure;  However, since the stages depend on each other, if no problems occur, after launching from first stage the whole campaign will be executed.

On the previous picture you can see that 'Commands' can be execute on the stage:
* Pause
* Resume
* Cancel

At this level, the commands will be executed on the jobs running at the Condor level.  If you choose to 'Pause' the jobs, keep in mind that at Resume , jobs will be re-submitted.  
Just an example of possible use of the 'Pause' at this level could be to free resources to launch another campaign that has become higher in priority for your accomplishments.

You can see in the following picture that the first stage has completed and the second stage started (top part of the picture) and then you can see that all three stages have completed.

![eve_test_submission_3together.png](/docs/images/eve_test_submission_3together.png)

From the following picture you can see that the 'gen' stage is finished and the stage 'sim' is in 'idle'  which could mean that part of the jobs are waiting in the queue to run.  
You can click on the Status History green button and see the various states and the transition time. 

![eve_test_submission_and_states_history.png](/docs/images/eve_test_submission_and_states_history.png)

To get more information on the submitted jobs for the stage, you can follow two paths:

* Go back to the Campaign page, click on the campaign to find all the stages and then click on the stage, in our case eve_test_gen_v1 .
* Go to the 'Campaign Stages' page, find the stage and click on it.

Either way you will end up on the Campaign Stage page from which you can view all the information about the stage and the jobs:

![eve_test_gen_v1_page.png](/docs/images/eve_test_gen_v1_page.png)

A lot of information is available for the stage and we will cover the details under the [Campaign Stages]({{< ref "#a-stylecolorgreencampaign-stagesa" >}}) section.  

However, worth of mentioning here, is the action 'Queue future job launches'.   
When we launched the campaign, as mentioned, if all goes well, all the dependent stages will start in _cascade_ based on the dependencies.   

This action allows you to _override_  the automatic flow. A useful scenario where the user might want to use this,  could be the following:  

* Our campaign has three stages; stage 1 produces output file that would be the input for stage 2. Let's say we want to check that the files have the correct format first before automatically continuing to stage2: this is where we would queue jobs for future launches for stage 2. When we are satisfied with our checks we can resume and the campaign will continue till completion of stage 3.

To monitor the jobs, you can go to the 'All Jobs' in Landscape and see the list of jobs running. The following  pictures illustrate  fifebatch monitoring page where you can see  jobs for the first two stages,  gen_v1 and sim_v1.

![eve_test_jobs.png](/docs/images/eve_test_jobs.png)


If a stage had some issues, for example not all jobs succeed, the batch job Status would appear as in the following pictures where we use a different campaign to show what to expect:

![eve_demo_stage_gen_fail.png](/docs/images/eve_demo_stage_gen_fail.png)

![eve_demo_stage_gen_fail_landscape.png](/docs/images/eve_demo_stage_gen_fail_landscape.png)


### Campaign Stages

Campaigns can have one or more stages. Stages represent different workflows for the campaign depending whether they are dependent on each other.  
To view all the stages you can select the 'Campaign Stages' option from the side panel or you can go to the Campaigns page and click on the campaign of interest and you will be redirected to the stages page.

![campaigns_to_stages.png](/docs/images/campaigns_to_stages.png)

The table shows campaigns and the associated stages. You can sort and filter to narrow down the choices. Clicking on a stage you will be redirected to the stage page where you can view all the stage information.  
This will be covered later on under the  [Campaign Stage Page Information]({{< ref "#a-stylecolormagentacampaign-stage-page-informationa" >}}) section.

#### Editing a Campaign Stage

Because the stage belongs to a campaign, to add or modify stages you will use the Campaign Editor.  
As an example, here we will keep using our eve_test campaign. From the picture you will notice the color coordination between each stage and the job type, just an easy visual way to associate the items.

![eve_test_final.png](/docs/images/eve_test_final.png)

The following picture shows how to view and edit the stage. As a side note, you might notice that the eve_test_gen_v1 stage position on the editor board has changed to better view the layout.  
Moving the elements around can be very useful when campaigns have many stages; the new position is saved as well when you save the campaign.

![eve_test_editor.png](/docs/images/eve_test_editor.png)

Let's examine in details the fields for a stage: some are text fields, some are pull-down menus and some upon 'Edit' will open other pop-up windows:

![eve_test_stage_form.png](/docs/images/eve_test_stage_form.png)

* <i id="Name">_Name_</i> : the name for this stage, which,  as a suggestion, it could be something meaningful for the purpose of the stage.

* <i id="VO-Role">VO Role</i>: the role to use for jobsub when submitting jobs for this campaign. It can be "Production", "Analysis" or in some cases "Calibration" or others provided they exist in the experiment VO role.

* <i id="Software-Version">Experiment Software Version</i>: the software version. Typically experiment software components are bundled up in a version to be used by the running jobs.  
The version will be set in the metadata of output files generated by this campaign.  
POMS assumes files have metadata that lists their parentage, and software application information; it can then use the software version, filename patterns, and parentage to define datasets for the output of this campaign layer.  
Please be advise for the following: To propagate this value to the launch of the jobs, the user need to either:  

  1. specify %(version)s  in the launch/setup command, example:

         source myexp.sh; setup myexpcode %(version)s;
  OR

  2.  specify it as override parameter when using fife_launch, example:

          fife_launch -c my.cfg -O global.version=$(version)s;

If you don't use either cases, the software version will be stored in the database as information for the stage BUT NOT used.


* <i id="Dataset">Dataset or split data</i>: Dataset this campaign stage will process. If this campaign is only ever run as a later stage in a workflow, this is ignored. This field is used in conjunction of the split type when appropriate.

* <i id="Split-Type">Dataset Split Type</i>:  It specifies how the Dataset could be split, please refer to [Dataset and Split Types]({{< ref "#a-stylecolormagentacampaign-stage-datasets-and-split-typesa" >}}) below for details.

* <i id="Completion-Type">Completion Type</i>:  This is where you specify the criterion  used to be able to move to next stage. Two options are available:
  * _Complete_:  to say the campaign layer submissions are complete when their jobs complete, or 
  * _Located_:  to say the layer is completed when the submissions output files are located.

* <i id="Completion-Percent">Completion %</i>:  This is related to the completion type: please advise that the percentage refers to the jobs in both cases:
  *  If you say that Completion type is 'complete' and specify a completion percent of 75%, then the campaign will move on to the next stage when 75% of the jobs are completed.
  *  If you say that Completion type is 'located' and specify completion percent of 75%, then the campaign will move on to the next stage when 75% of the jobs are completed AND 75% of  the output files are found in SAM.

* <i id="Parameter-Overrides">Parameter Overrides</i>:  This allows you to override parameters to your Job Type's launch command.  Clicking on the edit icon will pop up a window where you can specify the parameter as a key-value pairs that will be concatenated and put on the command line.  
  * Note that matching keys will replace matching keys from similar parameter lists you had previously assigned in the  _Definition Parameters_ in the Job Type.  
  * Note that the values in the Parameter Overrides will have [Variable Substitution]({{< ref "/variable_substitution" >}}) performed on them.

* <i id="Tes-Parameter-Overrides">Test Parameter Overrides</i>:  These are used in the same fashion as the _Parameter Overrides_ but only when Testing the campaign submission (see later).

* <i id="Depends-On">Depends On</i>: This lets you define the dependencies on other campaign layers that this one has. Note that to add a circular dependency (i.e. to make this campaign auto-launch the next submission as each one completes) you have to have saved the campaign at least once, so it will show up in the list of campaigns to choose from.

  * For example, if you had a campaign stage "stage1" that provides output for "stage2".  
  * You would want to define the previous stage, and the output file patterns to use on stage2 as:

        {"campaign_stages": ["stage1"], "file_patterns": ["%%"]} 

* _Login setup_: The login setup script to be used: a collection of bash commands to be executed on the experiment’s user defined host to establish the environment from which POMS will launch all jobsub jobs.
* _Job Type_: A Job 'Type' is a way of categorizing the job based on its purpose.

About _Parameter Overrides_: the following example shows how to override a parameter defined in the config file using the value of another existing parameter also defined in the config file:

Let's say we have the following in a config file:

    [global]
    ...
    outdir=/pnfs/exp/scratch/exppro/poms/jobs/outputs
    ...
    
    [job_test_output]
    
    job_outdir=/pnfs/scratch/exppro/



For a certain stage in our campaign then we want to override the _job_outdir_ with the value of _outdir_.
Then using the editor for the param override field, for the key-value pair we would specify the following:

key                                 value

    -Ojob_test_output.job_outdir         '%%(global.outdir)s'


so then when  you launch, in the command line it will look like:

    -Ojob_test_output.job_outdir='%(global.outdir)s'


The single-percent is for a POMS defined value, and the double-percent one is for a value in your config.

#### Campaign Stage Login/Setup and Job Type

The following picture shows the Login/Setups and the Jobtype for the current stage.
![eve_test_login_jobtype.png](/docs/images/eve_test_login_jobtype.png)

For details on the Login/Setup and Job Type definition please refer to [Compose a login setup]({{< ref "#compose-a-loginsetup-script" >}}) and [Compose a job type]({{< ref "#compose-a-job-type" >}}) sections.  
One thing to keep in mind is that the Login/Setup and the Job Type are now campaign specific, they may be shared by other campaigns; if this is the case and you want to change any of the fields in either components you will be asked to change the name not to affect other campaigns.

#### Campaign Stage Datasets and Split types

The following picture shows the available Split types.

![eve_test_stage_and_split_types.png](/docs/images/eve_test_stage_and_split_types.png)

Depending on the selection, more information can be filled in the form accordingly and also in the dataset or split type data field for which  an 'Edit' button may appear.  
For example, if you choose a split type of 'list' then you will use the dataset field to specify the list itself, see example below:

![eve_test_stage_list_split_type.png](/docs/images/eve_test_stage_list_split_type.png)

In this case three datasets have been assigned, ds1,ds2 and ds3 leading to use ds1 in first submission, ds2 in second etc etc.

Another interesting example for  split types is the 'multiparam' option:

![eve_test_stage_multip_split_type.png](/docs/images/eve_test_stage_multip_split_type.png)

When you select the multiparam split type and accept it, the field 'dataset or split type data' on the form appears with an additional 'Edit' button to pass the desired information.  
The final results is a list of lists: internally this is translated  in all the possible permutations which will be used, one per submission.  
Example, let's say you want to generate different particles at different energies to produce data to be used in particle simulation through a detector.  
So you could enter the following data:

![eve_test_ele_mu_multip.png](/docs/images/eve_test_ele_mu_multip.png)

This will generate the following permutations:
* electron_1GeV
* muon_1GeV
* electron_3GeV
* muon_3GeV

and if you submit the campaign, you could see in the log for the eve_test_gen_v1 stage, for example,  the following

    ....
    fife_launch -c cfg/generic.cfg --stage=gen  -Oglobal.    sam_dataset=electron_1GeV -Oglobal.release=v1_1 -Osubmit.N=5
    ....



and if you submit again:

    ....
    fife_launch -c cfg/generic.cfg --stage=gen  -Oglobal.    sam_dataset=muon_3GeV -Oglobal.release=v1_1 -Osubmit.N=5
    ....

If you try to submit more times than the number of permutations, in our case 4, you would get and error and to submit again,
from the stage page, you need to reset the 'Last Split', see the following snippet from the campaign stage page:

![eve_test_gen_no_split_error.png](/docs/images/eve_test_gen_no_split_error.png)


#### Campaign Stage Page Information

Each stage of a campaign has its own page with a lot of information:

![eve_test_stage_sim_page.png](/docs/images/eve_test_stage_sim_page.png)

The page has several sections organized by the type of information they present:

The top section shows plot generated within Landscape with statistics for the last 30 days.

* Job status for the campaign stage
* Reports
* Actions
* Campaign Stage general information
* Job Type information
* Login/Setup Information
* Diagram with stages  immediate dependencies.
* Links to Recent Launch Outputs.

Reports:
Two types of reports are available (for a more general description with pictures see the [Monitoring]({{< ref "#monitoring" >}}) section below:

* Reports generated and available from the Landscape web pages
  * Production Shifter Page:  a summary for the last 24 hours for your experiment of all the campaign/stages jobs.
  * POMS Campaign Stage: a summary for the last 30 days for the jobs for the selected stage.
  * Campaign Stage Stats: a summary for the last 30 days with efficiency ,memory requests and usage.
  * Submissions: a summary for the lat 7 days on the submission with details on the jobs.

* Reports generated within POMS.
  * Campaign Stage Submissions: by default it shows information for the last 7 days but you can navigate in the paste and future. You will see, for example, all the submissions history, current status and commands you could issue.
  * Campaign Stage Submission Files: a summary for the last 7 days on all the files for the jobs with SAM related information.


<p id="Actions">Actions:</p>

* Editing: this will redirect (for now) to the stage form not the editor, which we strongly suggest to use to update the stage.
* Launching the campaign: before starting it, you can view the commands that will be executed clicking on <i style="color:orange">commands</i> (see more information about this below):
  * If you are sure to start the campaign for production , you would  click on 'Launch Campaign Jobs Now',
  * If you prefer testing first  you can select the 'Launch Campaign Test Jobs Now'.
In either case, after confirming the launch, you will be redirected to the log page.   
The user can verify the status of the jobs choosing various options in the 'Reports/Status' section.
* Kill, Hold, Release jobs: these actions are to control the jobs on Condor.
* Schedule Future Job Launches: this is to setup a crontab job or use an existing one.
This page lets you generate/update a crontab entry to push the "launch jobs" button for this project on a schedule.  
If there is a current crontab entry , it shows in the labelled box, otherwise it shows "None". To set times, fill in the lower form with cron values (see the cron man page for more information).  
For example, to run it at 3:00 every morning, you would fill in 0 in Minutes, 0 in hours, and check All for the days of the week, and leave days of the month as an asterisk "*".  
Or to run it on the first of every month, set fill in as above, but set the days of the month to "1". Then hit "Submit", and the current crontab entry should update, and you have scheduled an entry.  
To stop submissions, the "Delete" button should clear the current entry to "None".
* Queue future jobs launches: This action allows you to _override_  the automatic flow for a campaign where stages depend on each other and the next stage would automatically start when previous has finished. A useful scenario where the user might want to use this  could be checking that output files from the current stage are correct before feeding them as input to the next stage.

* <i style="color:orange">About showing the commands</i> to launch: when you click on this, a page with the list of commands to be executed will appear: Please notice though , that a _submission id_ is reserved at this point in case you do decide to execute the commands and launch it.  
If after _commands_  you click on  _'Campaign Stage Submission'_ on the stage page, you will see the submission id for the stage and the status is _New_.  
If you decide NOT to submit using the commands, after two hours the launch will be automatically killed for you.  See the pictures below:

![f_demo_1_reco_cmds_list.png](/docs/images/f_demo_1_reco_cmds_list.png)

![f_demo_1_reco_submission_history_new.png](/docs/images/f_demo_1_reco_submission_history_new.png)


#### Cloning a Stage

From the Campaign Editor you can clone stages; using our eve_test campaign, if you right click on, for example, eve_test_gen_v1 stage, the 'Add Stage' pop up window will appear.  
An interesting way to clone, is simply to specify how many 'copies' of the original you want, 3 in the example, using the simple syntax  _*3_ :

![eve_test_cloning_stages.png](/docs/images/eve_test_cloning_stages.png)

and you will end up with three copies with a default naming conventions as shown in the next picture:

![eve_test_cloned_stages.png](/docs/images/eve_test_cloned_stages.png)

The new stages are created independently from each other and they all inherit the fields and definitions of the original stage.  
At this point you can edit them and do whatever you need. Remember to save them!

### Sample Campaigns

POMS provides few sample campaigns for the user to view and clone; then the cloned campaign can be saved and modified accordingly using the Campaign Editor.  
Few sample campaign are provided which you can view to see which one could fit your purpose. 

![clone_from_sample.png](/docs/images/clone_from_sample.png)

When you click on 'Clone Campaign' a pop up box opens up in which you specify the desired name for the cloned campaign.  
You will notice that the name you provided, in this case mvi_test, replaces the word 'sample' in the original campaign name.

![clone_sample_before_after.png](/docs/images/clone_sample_before_after.png)

Another thing to notice is that the stages have kept the original names: since stages belong to a campaign, duplicate names across campaigns are allowed.  
Remember to save the campaign before leaving the Campaign Editor.

### Monitoring

After launching a campaign you can monitor the jobs either viewing Kibana plots in Landscape or check the status within POMS.  
Let's use for our examples the campaign f_demo_2_mvi which has three stages:
* f_eg_v1
* f_sim_v1
* f_reco_v1

Launching the campaign it will launch the three stages in sequence, provided all goes well. To monitor we need to go the the first stage page (we will show just the top section of the page):

![Monitor_eg_v1_page.png](/docs/images/Monitor_eg_v1_page.png)

If you follow the link for _Campaign Stage Submissions_ you can see the leader job in a 'New' state:

![Monitor_eg_v1_submission_new.pn](/docs/images/Monitor_eg_v1_submission_new.png)

After a little bit, if you view the same page again, you can see that the stage is in running mode and you can click on History to see the transitions:

![Monitor_eg_submission_and_history.png](/docs/images/Monitor_eg_submission_and_history.png)


You can see jobs information also viewing Kibana plots in Landscape selecting the 'All jobs' option from the 'Jobs' menu item on the side panel:  
In the following picture you can see jobs for all the three stages

![Monitor_kibana_dashboard_all3stages.png](/docs/images/Monitor_kibana_dashboard_all3stages.png)

Then, checking the Campaign stage submissions again, you can finally see that all three stages have finished and you can view the individual history with the transitions times:

![Monitor_all_submissions_and_history.png](/docs/images/Monitor_all_submissions_and_history.png)


* ### [Campaign Editor]({{< ref "/campaign_editor" >}} )

* ### [Glossary]({{< ref "/glossary" >}})