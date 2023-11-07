---
layout: page
title: Re-factorization
---
* TOC
{:toc}

## poms_services.py

- def error_response():
- def popen_read_with_timeout(cmd, totaltime = 30):
- def init(self):
- def headers(self):
- def sign_out(self):
- def index(self):
- def es(self):

##### UtilsPOMS
- def quick_search(self, search_term):
- def jump_to_job(self, jobsub_job_id, **kwargs ):  
---
* TOC
{:toc}

##### CALENDAR
- def calendar_json(self, start, end, timezone, _):
- def calendar(self):
- def add_event(self, title, start, end):
- def edit_event(self, title, start, new_start, end, s_id): ##### even though we pass in the s_id we should not rely on it because they can and will change the service name
- def service_downtimes(self):
- def update_service(self, name, parent, status, host_site, total, failed, description):
- def service_status(self, under = 'All'):  
- def service_status_hier(self, under = 'All', depth = 0):
---
* TOC
{:toc}

##### DBadminPOMS
- def raw_tables(self):
- def user_edit(self, *args, **kwargs):
- def experiment_members(self, *args, **kwargs):
- def experiment_edit(self, message=None):
- def experiment_authorize(self, *args, **kwargs):  
---
* TOC
{:toc}

##### CampaignsPOMS
- def launch_template_edit(self, *args, **kwargs):
- def campaign_definition_edit(self, *args, **kwargs):
- def campaign_edit(self, *args, **kwargs):
- def campaign_edit_query(self, *args, **kwargs):
- def new_task_for_campaign(self, campaign_name, command_executed, experimenter_name, dataset_name = None):
- def show_campaigns(self,experiment = None, tmin = None, tmax = None, tdays = 1, active = True):
- def campaign_info(self, campaign_id, tmin = None, tmax = None, tdays = None):
- def campaign_time_bars(self, campaign_id = None, tag = None, tmin = None, tmax = None, tdays = 1):
- def register_poms_campaign(self, experiment, campaign_name, version, user = None, campaign_definition = None, dataset = "", role = "Analysis", params = []):
- def list_launch_file(self, campaign_id, fname ):
- def schedule_launch(self, campaign_id ):
- def update_launch_schedule(self, campaign_id, dowlist = None, domlist = None, monthly = None, month = None, hourlist = None, submit = None , minlist = None, delete = None):
- def mark_campaign_active(self, campaign_id, is_active):
- def make_stale_campaigns_inactive(self):  
---
* TOC
{:toc}

##### Tables
- def list_generic(self, classname):
- def edit_screen_generic(self, classname, id = None):
- def update_generic( self, classname, *args, **kwargs):
- def edit_screen_for( self, classname, eclass, update_call, primkey, primval, valmap): ##### ##### #####  Why this function is not expose  
---
* TOC
{:toc}

##### JobPOMS
- def active_jobs(self):
- def report_declared_files(self, flist):
- def output_pending_jobs(self):
- def update_job(self, task_id, jobsub_job_id, **kwargs):
- def test_job_counts(self, task_id = None, campaign_id = None):
- def kill_jobs(self, campaign_id=None, task_id=None, job_id=None, confirm=None):
- def jobs_eff_histo(self, campaign_id, tmax = None, tmin = None, tdays = 1 ):
- def set_job_launches(self, hold):
- def launch_jobs(self, campaign_id, dataset_override = None, parent_task_id = None): ##### ##### ##### needs to be analize in detail.  
---
* TOC
{:toc}

##### TaskPOMS
- def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor):
- def wrapup_tasks(self):
- def show_task_jobs(self, task_id, tmax = None, tmin = None, tdays = 1 ): ##### ##### #####  Need to be tested HERE
- def get_task_id_for(self, campaign, user = None, experiment = None, command_executed = "", input_dataset = "", parent_task_id=None):  
---
* TOC
{:toc}

##### FilesPOMS
- def list_task_logged_files(self, task_id):
- def campaign_task_files(self, campaign_id, tmin = None, tmax = None, tdays = 1):
- def job_file_list(self, job_id,force_reload = False): ##### ##### Ask Marc to check this in the module
- def job_file_contents(self, job_id, task_id, file, tmin = None, tmax = None, tdays = None):
- def inflight_files(self, campaign_id=None, task_id=None):
- def show_dimension_files(self, experiment, dims):
- def actual_pending_files(self, count_or_list, task_id = None, campaign_id = None, tmin = None, tmax= None, tdays = 1): ##### ##### ##### ??? Implementation of the exception.
- def campaign_sheet(self, campaign_id, tmin = None, tmax = None , tdays = 7):
- def json_project_summary_for_task(self, task_id):
- def project_summary_for_task(self, task_id):
- def project_summary_for_tasks(self, task_list):  
---
* TOC
{:toc}

##### TriagePOMS
- def triage_job(self, job_id, tmin = None, tmax = None, tdays = None, force_reload = False):
- def job_table(self, tmin = None, tmax = None, tdays = 1, task_id = None, campaign_id = None , experiment = None, sift=False, campaign_name=None, name=None,campaign_def_id=None, vo_role=None, input_dataset=None, output_dataset=None, task_status=None, project=None, jobsub_job_id=None, node_name=None, cpu_type=None, host_site=None, job_status=None, user_exe_exit_code=None, output_files_declared=None, campaign_checkbox=None, task_checkbox=None, job_checkbox=None, ignore_me = None, keyword=None, dataset = None, eff_d = None):
- def jobs_by_exitcode(self, tmin = None, tmax = None, tdays = 1 ):
- def failed_jobs_by_whatever(self, tmin = None, tmax = None, tdays = 1 , f = [], go = None):  
---
* TOC
{:toc}

##### TagsPOMS
- def link_tags(self, campaign_id, tag_name, experiment):
- def delete_campaigns_tags(self, campaign_id, tag_id, experiment):
- def search_tags(self, q):
- def auto_complete_tags_search(self, experiment, q):  
---
* TOC
{:toc}

##  ModulesPOMS

UtilsPOMS.py

    - def handle_dates(self, tmin, tmax, tdays, baseurl): ##### this method was deleted from the main script
    - def quick_search(self, dbhandle, redirect, search_term):

CalendarPOMS.py

    - def calendar_json(self, dbhandle,start, end, timezone, _):
    - def calendar(self, dbhandle):
    - def add_event(self, dbhandle,title, start, end):
    - def service_downtimes(self, dbhandle):
    - def update_service(self, dbhandle, log_handle, name, parent, status, host_site, total, failed, description):
    - def service_status(self, dbhandle, under = 'All'):

DBadminPOMS.py

    - def user_edit(self, dbhandle, *args, **kwargs):
    - def experiment_members(self, dbhandle, *args, **kwargs):
    - def experiment_edit(self, dbhandle):
    - def experiment_authorize(self, dbhandle, loghandle, *args, **kwargs):

CampaignsPOMS.py

    - def launch_template_edit(self, dbhandle, loghandle, seshandle, *args, **kwargs):
    - def campaign_definition_edit(self, dbhandle, loghandle, seshandle, *args, **kwargs):
    - def campaign_edit(self, dbhandle, loghandle, sesshandle, *args, **kwargs):
    - def campaign_edit_query(self, dbhandle, *args, **kwargs):
    - def new_task_for_campaign(dbhandle , campaign_name, command_executed, experimenter_name, dataset_name = None):
    - def show_campaigns(self, dbhandle, loghandle, samhandle, experiment = None, tmin = None, tmax = None, tdays = 1, active = True):
    - def campaign_info(self, dbhandle, loghandle, samhandle, err_res, campaign_id, tmin = None, tmax = None, tdays = None):
    - def campaign_time_bars(self, dbhandle, campaign_id = None, tag = None, tmin = None, tmax = None, tdays = 1):
    - def register_poms_campaign(self, dbhandle, loghandle, experiment,  campaign_name, version, user = None, campaign_definition = None, dataset = "", role =    "Analysis", params = []):  
    - def get_dataset_for(self, dbhandle, err_res, camp):
    - def list_launch_file(self, campaign_id, fname ):
    - def schedule_launch(self, dbhandle, campaign_id ):
    - def update_launch_schedule(self, loghandle, campaign_id, dowlist = None,  domlist = None, monthly = None, month = None, hourlist = None, submit = None , minlist =  None, delete = None):  
    - def get_recovery_list_for_campaign_def(self, dbhandle, campaign_def):
    - def make_stale_campaigns_inactive(self, dbhandle, err_res):

TablesPOMS.py

    - def list_generic(self, dbhandle, err_res, gethead, seshandle, classname):
    - def edit_screen_generic(self, err_res, gethead, seshandle, classname, id = None):
    - def update_generic( self, dbhandle, gethead, loghandle, seshandle, classname, *args, **kwargs):
    - def update_for( self, dbhandle, loghandle, classname, eclass, primkey,  *args , **kwargs): ##### this method was deleded from the main script
    - def edit_screen_for( self, dbhandle, loghandle, gethead, seshandle, classname, eclass, update_call, primkey, primval, valmap):
    - def make_list_for(self, dbhandle, eclass,primkey): ##### this function was eliminated from the main class.
    - def make_admin_map(self,loghandle): ##### This method was deleted from the main script.

JobsPOMS.py

    - def active_jobs(self, dbhandle):
    - def output_pending_jobs(self,dbhandle):
    - def update_job(self, dbhandle, loghandle, rpstatus, task_id = None, jobsub_job_id = 'unknown',  **kwargs):
    - def test_job_counts(self, task_id = None, campaign_id = None):
    - def kill_jobs(self, dbhandle, loghandle, campaign_id=None, task_id=None, job_id=None, confirm=None):
    - def jobs_eff_histo(self, dbhandle, campaign_id, tmax = None, tmin = None, tdays = 1 ):
    - def get_efficiency(self, dbhandle, loghandle, campaign_list, tmin, tmax): ##### This method was deleted from the main script
    - def launch_jobs(self, dbhandle,loghandle, getconfig, gethead, seshandle, err_res, campaign_id, dataset_override = None, parent_task_id = None):

TaskPOMS.py

    - def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor):
    - def wrapup_tasks(self):
    - def show_task_jobs(self, task_id, tmax = None, tmin = None, tdays = 1 ): ##### ##### #####  Need to be tested HERE
    - def get_task_id_for(self, campaign, user = None, experiment = None, command_executed = "", input_dataset = "", parent_task_id=None):
    - def task_min_job(self, task_id):

FilesPOMS.py

    - def list_task_logged_files(self, dbhandle, task_id):
    - def campaign_task_files(self, dbhandle, loghandle, samhandle, campaign_id, tmin = None, tmax = None, tdays = 1):
    - def job_file_list(self, dbhandle, jobhandle, job_id, force_reload = False): ##### ##### Should this funcion be here or at the main script ????
    - def job_file_contents(self, dbhandle, loghandle, jobhandle, job_id, task_id, file, tmin = None, tmax = None, tdays = None):
    - def format_job_counts(self, dbhandle, task_id = None, campaign_id = None, tmin = None, tmax = None, tdays = 7, range_string = None): ##### ##### This method was deleted  from the main script  
    - def get_inflight(self, dbhandle, campaign_id=None, task_id=None): ##### This method was deleted from the main script
    - def inflight_files(self, dbhandle, status_response, getconfig, campaign_id=None, task_id=None):
    - def show_dimension_files(self, samhandle, experiment, dims):
    - def actual_pending_files(self, dbhandle, loghandle, count_or_list, task_id = None, campaign_id = None, tmin = None, tmax= None, tdays = 1):
    - def campaign_sheet(self, dbhandle, loghandle, campaign_id, tmin = None, tmax = None , tdays = 7): ##### maybe at the future for a  ReportsPOMS module
    - def get_pending_for_campaigns(self,  dbhandle, loghandle, samhandle, campaign_list, tmin, tmax):
    - def get_pending_for_task_lists(self, loghandle, samhandle, task_list_list):

TriagePOMS.py

    - def job_counts(self, dbhandle, task_id = None, campaign_id = None, tmin = None, tmax = None, tdays = None): ##### ##### #####  This one method was deleted from the main script  
    - def triage_job(self, dbhandle, job_id, tmin = None, tmax = None, tdays = None, force_reload = False):
    - def job_table(self, dbhandle, tmin = None, tmax = None, tdays = 1, task_id = None, campaign_id = None , experiment = None, sift=False, campaign_name=None, name=None,campaign_def_id=None, vo_role=None, input_dataset=None, output_dataset=None, task_status=None, project=None, jobsub_job_id=None, node_name=None, cpu_type=None, host_site=None, job_status=None, user_exe_exit_code=None, output_files_declared=None, campaign_checkbox=None, task_checkbox=None, job_checkbox=None, ignore_me = None, keyword=None, dataset = None, eff_d = None):  
    - def failed_jobs_by_whatever(self, dbhandle, loghandle, tmin = None, tmax =  None, tdays = 1 , f = [], go = None):

TagsPOMS.py

    - def link_tags(self, ses_get, dbhandle, campaign_id, tag_name, experiment):
    - def delete_campaigns_tags(self, dbhandle, ses_get, campaign_id, tag_id, experiment):
    - def search_tags(self, dbhandle, q):
    - def auto_complete_tags_search(self, dbhandle, experiment, q):