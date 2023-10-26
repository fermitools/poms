---
include_toc: true
show_comments: false
toc: true
---
## v4_1_1
### Notable Features
* Within a campaign, any stage can be configured to wait for approval  before being submitted.
* Documentation has been greatly enhanced to provide greater details.  For example stages have been enlarged with a more complete description.
* Each webpage is now linked to a wiki help page.   The link is shown as a question mark at the top of the page.  

### Bugs Fixed
* Bug #22243: kill submission, etc. get worker jobs, but not dagman, also should set submission to "Removed"
* Bug #22234: Transition to Located on single file projects
* Bug #22162: cloning sample files needs to fix experiment= and actually fix the name
* Bug #22073: Job submitted, failed to run successfully but POMS so no data file generated, but POMS marked them located
* Bug #22064: editing a login/setup entry for analysis campaign requires an experiment hostname
* Bug #22047: upload_file page without choosing file
* Bug #22012: Got errors while killing a running job

### New Features
* Feature #22110: create a pause/hold feature while submitting stages in the same campaign
* Feature #22090: poms_client API for Campaign Stage Submissions
* Feature #22026: show_campaigns and list_campaigns
* Feature #22021: Special use of UPLOADS env var for Analysis users
* Feature #22015: Override-able parameters in the gui editor: Database Change
* Feature #21955: poms client submit a job by campaign
* Feature #21953: poms_client add function upload ini should return the campaign and all stage ids/names
* Feature #21952: poms_client make_poms_call
* Feature #22177: poms_client APIs get job_type_snapshot_obj and login_setup_snap_obj by their IDs
* Feature #22026: show_campaigns and list_campaigns
* Feature #21993: create a campaign from ini
* Feature #22176: POMS Client API for campaign names and stage names giving campaignid and.or stage_id
* Feature #22213: extend poms_client campaign_stage_submissions function to take as argumrents 'tdays', 'tmin' and 'tmax'
* Feature #22035: poms_client: remove functions needed

### New Tasks and Ideas
* Task #21967: poms_client: clean up the user interface to reflect the change of campaign and campaign stage definition
* Idea #22046: How to handle dataset for intermediate stages
* Task #21555: Add link to Campaign Documentation on POMS home page
* Task #21477: Add one info page where multiple ? are present
* Task #21046: Document Handling custom placeholders in param overrides
* Task #19708: Documentation: Recovery Campaigns
* Task #19707: Documentation: Launch Script


## v4_1

### Major Features
* Added support for Analysis users.  We are testing this in production before distributing across experiments.


### Bugs Fixed
* Bug #20619: Need a check on campaign stage edits
* Bug #20865: GUI Campaign editor "Double click to open" box remains too long.
* Bug #20904: Tags form fails creating new tags.
* Bug #20978: Need time range limit in poms_submission_agent.py...
* Bug #21045: Campaign stage form - two links point to same plot.
* Bug #21341: INC000000993451 - Users crashing when changing campaigns - but logged into different experiment.
* Bug #21380: campaign_task_files - the previous/next day links are not working
* Bug #21401: Need to add knowledge of experiment in campaign_time_bars .
* Bug #21402: When using form from Compose campaign stage, no error reported if stage has no login setup or job type.
* Bug #21413: datasets generated for dependent job launches should be more restrictive.
* Bug #21605: Kill / hold / release jobs doesn't hold right on DAGs (RITM0761732)
* Bug #21707: Campaign editor: cs_split_type does not save changes
* Bug #21775: Deleting tagged campaign fails


### New Features
* Feature #19128: Revise user and experiment forms to follow user entry procedures.
* Feature #20213: Multiparameter workflow
* Feature #20217: Tool to interactively setup the same environement as a job launch
* Feature #20234: Report failure of a POMS campaign submission in the offline dashboard of experiments.
* Feature #20244: Maintain campaign_stage_n_active dataset
* Feature #20590: Workflow editor, internal structure
* Feature #20763: Make status (i.e. for SubmissionHistory) a vocab table with ids ordered(New, LaunchFailed, Idle, Running, Held, Failed, Completed, Located)
* Feature #21103: Submissions: Code update
* Feature #20771: Add button for custom recovery launches somewhere...
* Feature #20852: add time frame for stage plots
* Feature #20881: set X509_USER_PROXY auomatically for production jobs
* Feature #20944: Make a tag selection searchable.
* Feature #20993: Add a launch campaign button from Campaign page
* Feature #21109: Expose POMS "hidden" data to users
* Feature #21207: Need recovery launch info in .ini file dump, workflow editor, etc.
* Feature #21208: Need recovery launch info in .ini file dump
* Feature #21209: need recovery launch info in campaign editor on job type
* Feature #21210: Add recovery launch bits to ini file uploader.
* Feature #21282: Campaign Editor: Cloning Stages
* Feature #21411: Delete a job in New status
* Feature #21420: Update poms_client to pass POMS4_XXX parameters to jobsub
* Feature #21445: Request RITM0739824: login_setup and job_type are not highlighted in aqua for the campaign stage.
* Feature #21502: Analysis user support
* Feature #21503: File upload facility for analysis users
* Feature #21504: Sandbox directory creator
* Feature #21505: Modify launch code for analysis sandboxes
* Feature #21506: Amend login/setup for Analysis jobs
* Feature #21593: restrict login/setup launch_host and launch_user for analysis users
* Feature #21507: Add a file / proxy upload script to poms_client
* Feature #21508: Modify launch code to hold launches for analysis users without a proxy
* Feature #21613: Replace list of launch logs with submission list in campaign info page
* Feature #21701: About Campaign Editor.
* Feature #21702: About Campaign Editor.
 
### New Tasks
* Task #19706: Documentation: Launch Template
* Task #20598: Split type plugins
* Task #20995: Add check on campaign stage edits
* Task #20996: Add check on campaign stage edits
* Task #21102: Submissions: Database changes
* Task #20855: Search Box - Campaigns and stages forms
* Task #20887: Obtain coordinators from FERRY.
* Task #20902: Move active flag from stages to campaigns
* Task #20941: Store nodes positions in campaign editor.
* Task #21187: Add campaign name and stage name to form produced by list_launch_file.
* Task #21372: Testing campaigns show up in uboone production reports
* Task #21476: type fields in campaigns, campaign_stage
* Task #21436: Documentation: Experiment version number
* Task #21486: Revise to the submission time bars page
* Task #21509: Convert Coordinator to Superuser
* Task #21510: Convert the submission time bars to tables
* Task #21523: Make Campaign field readonly when editing an existing stage.
* Task #21526: Rename function to compose job type.
* Task #21697: About Uploaded Files option.
* Task #21698: About Uploaded Files page.
* Task #21699: About Launch button.

 

## v4_0_2

### Major Features

* Added new split type for draining datasets.
* Added button to force submissions to move to located.

### Bugs Fixed

* Bug #21336 wrapup_tasks using wrong file pattern,,,
* Bug #21296 Provide user understandable data for a Campaign Stage Launch 404 Error.
* Bug #21167 Corrected dependent/recovery jobs to store and show correct user.
* Bug #21166 Fixed recovery bugs caused by invalid dimension query.

### New Features/Tasks

* Feature #21318 Button to force submissions to move to Located
* Feature #20243 Added byexistingruns split type.

## v4_0_1

### Major Features

* Backward compatibility documentation provided "here":https://cdcvs.fnal.gov/redmine/projects/prod_mgmt_db/wiki/Release_v400_Backward_Compatibility_-_What_You_need_to_know
* Campaign Editor has had minor improvements for easier usage.
* Campaign page active/inactive setting now works.
* Tagging campaigns is working again.

### Bugs Fixed

* Bug #20916 Spaces in stage/job type/login setup names
* Bug #20917 Multiline launch scripts in login/setup forms
* Bug #20935 Fix code that handles Tags
* Bug #20936 Campaign Editor: prevent linking the campaign defaults to the stage

### New Features/Tasks

* Feature #20938 Campaign Editor improvements
* Feature #20939 Documentation: talk about 'backward compatibility' for new release


## v4_0_0

### Major Features

### Bugs Fixed

* Bug #19699 Unregistered user logging in causes stack dump.
* Bug #19745 Aliases fail when shibboleth session has not be established.

### New Features/Tasks

* Documentation Revised - Written for new users
* Fully integrated with Fifemon
* Experiment's Analysis users can login and view production
* .ini files can be exported, edited and uploaded
* NEW <a style="color:blue"><b>GUI Campaign Editor</b></a> - View/edit entire campaign.
* Feature #20654 Previous Users should be able to *view* anything in their experiment
* Feature #20673 Upgrade POMS to python 3.6
* Task #19187 Tutorials for experiment's production teams.
* Task #19276 Give Landscape the info of exp for the specific CAMPAIGN_ID, Campaign NAME, user running campaign
* Task #19694 Documentation: Overview Documentation
* Task #19696 Documentation: Production Processing High Level View
* Task #19701 Documentation: Requirements for Creating a Campaign
* Task #19698 Documentation: Adding a New Experiment or User
* Task #19702 Documentation: Creating a Campaign - Details
* Task #19710 Documentation: Campaign Actions
* Task #19770 Documentation: Cloning
* Task #19758 Create Campaign Stage Dependency Ordering
* Task #19263 Webpage in POMS to build the workflow
* Task #19761 Test Workflow Editor
* Task #19837 Fix up prototype workflow editor
* Task #19931 Automatically add users to poms_announce
* Task #20037 Table/field ids renaming
* Task #20041 Conversion of POMS to Fifemon
* Task #20042 Conversion: Database Changes
* Task #20043 Conversion: Eliminate scraping applications
* Task #20045 Conversion: Convert reports/plots to use elasticsearch
* Task #20101 Rename the tables and fields and update all source code to match functional names.
* Task #20225 Update the Show Campaigns (tags) form to use a table.
* Task #20226 Revise the main page.


## v3_1_0

### Bugs Fixed

* Bug #19445 SAM project link not working in POMS - Opened SNOW ticket INC000000935297
* Bug #19624 Whitespace confuses split_types code
* Bug #19922 bulk update is locking to heavily...
* Bug #19852 Stack dumps when deleting a job type
* Bug #19704 Jump to Campaign from the Campaign Stage Info only works for Active campings.
* Bug #19647 The link to the SNOW downtime page appears different from the other two under external links in POMS menu (blue characters and not centered).
* Bug #19624 Whitespace confuses split_types code

### New Features/Tasks

* Feature #19504 Dependencies should accept SAM metadata query bits, not just file pattern.
* Feature #19434 Use the Exp Software Version to tell POMS which version of the experiment's code would be loaded in the job.
* Feature #19418 Add link to the SNOW outage calendar under external links menu
* Feature #19324 add completed jobs index
* Feature #19199 Add "Test mode" to the Campaign stage.
* Feature #17852 Get downtimes from SNOW downtime calendar
* Task #19256 Fix .ssh/config for the round-robin alias

## v3_0_0

### Bugs Fixed

* Bug #18627: submission time bars prev/next doesn't work for tag= instead of campaign_id=.
* Bug #18833: Restore the fuzzy search for users.
* Bug #18918: "Tag/Untag Campaigns" action not working (as expected?)..
* Bug #18945: addusers.py does not update POMS when VOMS data for existing experimenter changes.
* Bug #19029: addusers.py seems to switch to inactive users with production role.
* Bug #19055: People with role production in VOMS have analysis role in POMS.
* Bug #19139: Failed jobs classified as located in dbrailsf_gen_poms_test campaign stage.
* Bug #19142: Kill submission campaign not working.
* Bug #19192: Test button not working on just-added launch templates.
* Bug #19338: declared_files_agent is starving...Bug #19368: release job is killing instead..

### New Features/Tasks

* Feature #16747: Show dependency, if any, in the campaign info page.
* Feature #18074: Last-modified info on campaigns, etc.
* Feature #18123: Need a way to reset, step back, split dataset iterators...
* Feature #18830: Role pull down menu for User Authorization and add security check.
* Feature #19297: Add "Config File Templates" in the POMS navigation menu.
* Feature #19024: addusers.py development.
* Feature #19120: DNS round-robin alias dune-computing.fnal.gov as a job launch node in POMS. 
* Task #18871: Integration database.
* Task #19298: Add configuration file templates to the "Config File Templates" sub-menu.
* Task #18911: Provide a launch config template to interface POMS to the experiments' configurations, job submission and executable.
* Task #18988: Download/upload whole campaign/workflow to/from file.
* Task #19088: Add coordinators to each of the experiments according to the partial list in the description (Will be update as the info come).
* Task #19198: Change "Submit" button in all Configure Work with "Save"


## v2_3_0

### Bugs Fixed

* Bug #18176: Don't fail if session_experiment is not set.
* Bug #18194: Hold reason not being propagated to database.
* Bug #18300: Deal with files with unknown status in project summary/datasets for "delivered" file stats.
* Bug #18361: Button to reset split dataset sequence in a campaign.
* Bug #18388: Held jobs that are removed should be marked as Removed, not Completed.

### New Features

* Feature #12195: Test button for launch template, campaign definition.
* Feature #17741: Add "Tag/UnTag Campaign".
* Feature #17779: Add "campaign_type" field to "campaigns" table &model.
* Feature #17894: More datasets splitting types. 
* Feature #17895: Make links on campaign_info page links to last active week of campaign, not current week.
* Feature #17931: Add "HOLD/RELEASE Jobs button".
* Feature #17934: Add "HOLD/RELEASE Launches button(s)".
* Feature #18365: Add a byrun split type.
* Feature #18731: Track who launches/submits jobs including the cron launcher.

### Improvements

* Feature #17859: Define roles into POMS.
 * Task #18772: Change "root" from role to flag.

## v2_2_1

### Bugs Fixed

* Bug #17638: Deadlock errors in bulk_update, with Nova's 30k jobs queued.
* Bug #17641: Memory overrun from jobsub_q_scraper -* uwsgi on pomsgpvm02.
* Bug #17684: Crontab code is generating wrong path to the launcher again.
* Bug #17685: Bulk_update jobs doesn't collapse task_ids in task_updates.

## v2_2_0

### New Features

* Feature #17599: Additional histograms of job information -- run-time, transfer-time, etc.
* Feature #17349: job reporters should split updates into multiple queues by task_id (odd/even, modulo 3, etc).
* Feature #17324: Have in the main campaign list webpage only basic information.
* Feature #17323: New campaign should invalidate the cache.
* Feature #17188: Select bunch of campaings and mark them inactive capability.
* Feature #17187: Filter to order campaign list.
* Feature #17026: Color code numbers in job tables.
* Feature #17025: Add more file totals to show_campaigns -- we have pending files, but should have total in datasets, delivered, etc. rollups.
* Feature #16869: Interface to SNOW request and incident ticket.
* Feature #16782: Interface to ECL.
* Feature #16709: Link to some Kibana stats for each campaign.

### Improvements

* RITM0582786 from Alex. Run time and file transfer time histograms like job efficiency histogram.
* RITM0583138 from Alex. Implement the capability to select a bunch of campaigns from the list displayed and mark them inactive.

### Bugs Fixed

* Bug #17532: Unknown error when clicking Config work/compose Job type, the create a new job type.
* Bug #17525: Inactive campaigns (No activity in the latest x 7? days) shown as active.
* Bug #17524: Unknown error when clicking Campaign Data/campaign Stages to delete.
* Bug #17522: unknown error when clicking on configure work/Compose launch template to duplicating with the existing name.
* Bug #17506: limit launch template hosts to have experiment name in them.
* Bug #17327: bulk_update_jobs is cpu_bound and not keeping up.
* Bug #17284: Spreadsheet report page choked on large Campaigns.
* Bug #17264: project descriptions not going in	Marc Mengel.
* Bug #17259: Delete function not working in the web application.
* Bug #17258: Strip spaces from fields before storing - causing apparent duplication issues.
* Bug #17204: Have the full string "Launch Template" readable from the POMS campaign info.
* Bug #17200: New campaigns don't appear in the list.
* Bug #15181: Campaign Layer Sheet - - cannot click through.


## v2_1_2

### Bugs Fixed

* Bug #17028. Proxy Error while loading POMS pages is appearing again. NOvA is experiencing this problem and opened a ticket (RITM0575905).
  *  Bug #16989. Rework show_campaigns.
* Bug #17024. Fix headers in show_campaigns.
* Bug #17027. Unsual Completed but not Located job counts for NOvA.
* Bug #17085. Use N/A to indicate when information in a certain table cell is not provided. 
  *  Bug #17086.  Use N/A instead of -1 in poms/campaign_sheet pending column.


## v2_1_1

### New Features

* Feature #14287 More detailed overall campaign-at-a-glance status page 

### Improvements

* Request #15560Alex H. suggests to have a 1-week time window in the default page of active campaigns and mark those with 1 week of inactivity as inactive with the default time window equal the duration of the campaign.

### Bugs Fixed

* Bug #11963. Accessing development POMS application on fermicloud045, occasionally I see this error message:
  * The proxy server received an invalid response from an upstream server.
  * The proxy server could not handle the request GET /poms/campaign_sheet.
* Bug #15042: 'IntegrityError' is not defined
* Bug #15043: The link on the number for Total Completed jobs on poms/show_campaigns page returns an empty set.
* Bug #15545: Non exit code showing.  Not reporting an exit code via ifdh log, or the joblog_scraper agent being down/out of memory when the job was running.  Now we may get the exitcode, etc. from the joblog parser 
* Bug #15547: In cut versions, we print a "fatal" git error trying to get the version from git.
* Bug #15646 fts_scanner error on startup
* Bug #16403 Changed tag to version
* Bug #16405 Info in a time window are not correct.


## v2_1_0

### New Features

* Non-new features in this version. 

### Improvements

* Feature #16067: Use "streaming" mode in queries to reduce memory usage
* Feature #15058. We need several software test-stands:
* Feature #15144: Add mock webservice for testing agents	
* Feature #15208: add tests to test/testUtilsPOMS.py		
* Feature #15317: Add more tests to test/test_JobsPOMS.py	
* Feature #15342: Add tests to test/test_AccessPOMS.py		
* Feature #15343: add more tests to test/test_CalendarPOMS.py	
* Feature #15344: Add more tests to test/test_CampaignsPOMS.py	
* Feature #15345: Add mor tests to test/test_FilesPOMS.py	
* Feature #15346: Add more tests to test/test_TagsPOMS.py	
* Feature #15347: test_TriagePOMS.py	Closed	Marc Mengel	
* Feature #15348: Add test to stand up a cherrypy instace and fetch one each
* Feature #15624: Add username column to experimenters table to fix VOMS import

### Bugs Fixed
* Bug #15751. The Tag is not working. This was discovered by Margarita when she did the test of this module.


## v2_0_1

### New Features

* Non-new features in this version. This is a bug release. 

### Improvements
* Feature #15945: Change "Campaign Layers" to "Campaign Stages" in page templates
* Feature #15946: Hide unused files-per-job fields
* Feature #15948: Change name to Campaign Layers
* Feature #15949: Splitted menu into: "Campaign info" and "Configure Work"
* Feature #15950: Change the order of the steps into "Configure Work" menu in to make the options more intuitive.

### Bugs Fixed

* Bug #15866: Moving task updates out of update_job_common, and handle them differently in bulk_update_job.
* Bug #15867: Updated some code to make poms installable via setup.py; which  included a local poms
* Feature #15947: In main menu, get the composition/edit links under a different heading from info links


## v2_0_0

### New Features

* Feature #14270: Allow users to run arbitrary executables/scripts and workloads through the system via arbitrary launch scripts/executables
* Feature #14271: Support the model of sending data to jobs
* Feature #14272: Supports data externally pre-staged manually at sites (i.e. different site local caches)
* Feature #14274: Have appropriate permissions and privileges implemented to authorize each operation

### Improvements
* Feature #14248: Now we are using Elasticsearch to fetch job data.
* Feature #15044: Allow editing of param_overrides on campaign recoveries through UI

### Bugs Fixed
* Bug #15007: If we get a repeated error (all retries fail) on a SAM, POMS Stop querying. 
* Bug #15349: The job_log scraper had its reporting threads crashing and stopped actually reporting things.
* Bug #15332: Some jobs have two job_histories records for one job.
* Bug #15501: Campaign page breaks if accessed w/o authentication.
* Bug #15499: In output_pending_jobs POMS only lists files that match the output format for the job type. Furthermore, update_job only flags files as output files (and not log files, etc.) if they match the output format
* Bug #15447: Database deadlock error


## v1_0_1

### New Features

* Update fife_launch in fife_utils to integrate poms_client calls #14471:

### Bugs Fixed

* Statistics pages use campaign history to pick software version avoiding change the version in the whole dataset. #12948
* The snapshot id field is filled when the task is created.  #13888 
* Active for _experiement_ showed experiments you were not in. Currently, it check if the user is actually a member. #14463
* The incorrect Job_Type and Launch Template's names for all NOvA's campaigns were corrected using one for NOvA #14464
* Incorrect info for Launch Host, Launch Account and launch Setup for NOvA campaigns #14465
* Change the default analysis VO in the campaign definition to production VO #14554
* Assure that when the task go to located the jobs also go to located #14476


## v1_1_0

### New Features

* Ability to get reports about how effective/efficient are the campaigns #13699 
* Add to the Campaign (Layer) two fields: completion_type and completion_threshold. Feature #12751:  
* Feature #14752: Add job log parser to get final job info
* The runtime,memory, and disk were included in the Campaign Def/Campaign info #12966 
* Poms should keep a list of job launches requested while job launches are "held" #14659
* POMS hold job launches when services are "red" on dashboard. Feature #14660:
* Poms keeps a list of job launches requested while job launches are "held" Feature #14659:
* Add a script to extract VO Role=Production user info.  #14747
* POMS is integrated with SAM putting information into SAM db about campaign and task id, then  the info is fetched faster than using project descriptions Feature #14504
* New screen for a campaign "why is my job not running?". Integrate it with fifemon #12965

### Improvements

* The HTML templates were renamed to line up with the URL's, so they are easier to find. #12933
* Improve in the labeling of the job status, now the table describe the complete jobs with the total number, number of jobs located and number of jobs non-located.. #14477
* A complete refurbish of the POMS code in order to split in modules by functionality, having independent test units. #12934
* The  Campaign Info page was updated for new fields added to Campaign(Layers), LaunchTemplates, etc. #14531
* Better split types were added allowing the user to divide the dataset for the campaign  #13707 (In testing) 

### Bugs Fixed
* The code for pending campaigns made a query that didn't work if you did the whole campaign.  Furthermore, the generation of these dimension strings was factored out to common routines #13889
* When the jobs dropped out of the queue condor_q no longer suffices. Now it is stored into the elasticsearch logs.  #14376 
* Task completion/launching dependencies/recoveries not happening Bug #14866
* The uwsgi threads in development do not seem to be obviously leaking memory anymore either. Bug #14886
* Now it has a sub-item for each experiment in persistent DCache, and each goes to degraded at 80% and Failed at 90%. If enough areas fill up, we hold job launches. Bug #15008


## v1_0_0a

### New Features

* Include pending file counts on Active Campaign summary #13734
* Including some small features requested by NOvA as: change in campaign info stat time frame, button interface to mark campaign as inactive, specify output patterns in poms. #13849
* Solved the problem that efficiency histogram convolved low-efficiency jobs with jobs with unreported data, giving the wrong metric. #14245
* Compose CampaignLayer/JobType/LaunchTemplate have now a clone button, so you can clone an existing one and fix it up. #14251

### Improvements
* Update Terms Campaign-Layer/Submission/JobType versus Campaign/Task/CampaignDefinition to make it more clear for experimenters.

### Bugs Fixed

* Clean up compose CampaignLayer/JobType/LaunchTemplate in order to make the interface visually better for the users. #14249
* Reporting agents retrying on 404 errors forever in development #14246
* A bug that prevent submissions go into Located status #14242
* The code that was doing condor_q was adapted to be consistent with the changes made in jobsub #14247
* The problem of incorrect dataset definition for dependent job submissions was corrected.   #14243
* Correction of the bug that was showing zero pending files for all campaigns #14250
* After the upgrade of the single sing-on the login code in fifemon status reader was no longer working #14332


## v1_0_0

### New Features

* Add series of recovery types to campaign definitions; and to say one campaign depends on another. #12730
* Add CampaignDependency items that attach this campaign to other campaigns, along with a file-pattern-match for what output files from the previous campaign we want (default "%" for match-any). #12892
* Self dependencies run the next batch as soon as it finish. #13278

### Improvements

* Method in poms_service.py that looks for Campaigns that have had no tasks in the last week, and turns off their active flags. #12890
* Add CampaignRecovery entries attached to the CampaignDefinition, in order by "recovery_order" field, choosing recovery types from the RecoveryType table when you add entries. #12891
* After a job is marked as held then the system will kill the job. #12975

### Bugs Fixed

* The job launch command timeout in order it doesn't run forever. #12758
* The failed_jobs_by_whatever method loses tdays when you pick fields. The problem was solved.  #12931
* Some pages are missing date picker/forward-back links #12932
* Launch template edit was failing with double quotes in setup block #13590

## b0_5

### New Features

* Adding files-by-submission page showing requested,delivered, etc. #12193
* Each display page should handle time ranges  #11386
* Campaign should launch jobs for campaigns that depend on this one. #12721
* Services status dashboard. #12620
* File-based Task status page #12193
* task bars and job bars need jobsubjobid, not internal task/job_id. #11382
* monitor how full dcache persisitent partitions are via webpage #12727

### Bugs Fixed

* joblog scraper was missing input files sometimes #12290
* Getting the correct percent efficiency for jobs
* build a status page with file-based stats as requested by OPOS
* Fix timezone shift in status pages #11379
* job reporter code did not retry properly on connection errors #12722
* Getting constraint errors when updating jobs frequently #12723
* Support older idfh better in joblog_scraper #12922




