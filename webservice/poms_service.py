import cherrypy
from cherrypy.lib import sessions
import glob
import os
import time
import time_grid
import json
import urllib
import socket
import subprocess
import select
from collections import OrderedDict

from sqlalchemy import (Column, Integer, Sequence, String, DateTime, ForeignKey,
    and_, or_, not_,  create_engine, null, desc, text, func, exc, distinct
)
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, tzinfo,timedelta
from jinja2 import Environment, PackageLoader
import shelve
from poms.model.poms_model import (Service, ServiceDowntime, Experimenter, Experiment,
    ExperimentsExperimenters, Job, JobHistory, Task, CampaignDefinition,
    TaskHistory, Campaign, LaunchTemplate, Tag, CampaignsTags, JobFile,
    CampaignSnapshot, CampaignDefinitionSnapshot, LaunchTemplateSnapshot,
    CampaignRecovery,RecoveryType, CampaignDependency
)

from utc import utc
from crontab import CronTab
import gc
from elasticsearch import Elasticsearch
import pprint
import version


global_version="unknown"

import CalendarPOMS
import DBadminPOMS
import CampaignsPOMS
import JobsPOMS
import TaskPOMS
import UtilsPOMS
import TagsPOMS
import TriagePOMS
import FilesPOMS
import AccessPOMS
import TablesPOMS


def error_response():
    dump = ""
    if cherrypy.config.get("dump",True):
        dump = cherrypy._cperror.format_exc()
    message = dump.replace('\n','<br/>')

    jinja_env = Environment(loader=PackageLoader('poms.webservice','templates'))
    template = jinja_env.get_template('error_response.html')
    path = cherrypy.config.get("pomspath","/poms")
    body = template.render(current_experimenter=cherrypy.session.get('experimenter'),
                            message=message,
                            pomspath=path,
                            dump=dump,
                            version=global_version)

    cherrypy.response.status = 500
    cherrypy.response.headers['content-type'] = 'text/html'
    cherrypy.response.body = body.encode()
    cherrypy.log(dump)



class poms_service:


    _cp_config = {'request.error_response': error_response,
                  'error_page.404': "%s/%s" % (os.path.abspath(os.getcwd()),'/templates/page_not_found.html')
                  }

    def __init__(self):
        self.jinja_env = Environment(loader=PackageLoader('poms.webservice','templates'))
        self.path = cherrypy.config.get("pomspath","/poms")
        self.hostname = socket.getfqdn()
        self.version = version.get_version()
        global_version = self.version
        self.calendarPOMS = CalendarPOMS.CalendarPOMS()
        self.dbadminPOMS = DBadminPOMS.DBadminPOMS()
        self.campaignsPOMS = CampaignsPOMS.CampaignsPOMS(self)
        self.jobsPOMS = JobsPOMS.JobsPOMS(self)
        self.taskPOMS = TaskPOMS.TaskPOMS(self)
        self.utilsPOMS = UtilsPOMS.UtilsPOMS(self)
        self.tagsPOMS = TagsPOMS.TagsPOMS(self)
        self.filesPOMS = FilesPOMS.Files_status(self)
        self.triagePOMS=TriagePOMS.TriagePOMS(self)
        self.accessPOMS=AccessPOMS.AccessPOMS()
        self.tablesPOMS = TablesPOMS.TablesPOMS(self, cherrypy.log)


    @cherrypy.expose
    def headers(self):
        return repr(cherrypy.request.headers)


    @cherrypy.expose
    def sign_out(self):
        cherrypy.lib.sessions.expire()
        log_out_url = "https://" + self.hostname + "/Shibboleth.sso/Logout"
        raise cherrypy.HTTPRedirect(log_out_url)


    @cherrypy.expose
    def index(self):
        template = self.jinja_env.get_template('index.html')
        return template.render(services=self.service_status_hier('All'),current_experimenter=cherrypy.session.get('experimenter'),

        launches = self.taskPOMS.get_job_launches(cherrypy.request.db),
                               do_refresh=1, pomspath=self.path, help_page="DashboardHelp", version=self.version)


    @cherrypy.expose
    def es(self):
        template = self.jinja_env.get_template('elasticsearch.html')

        es = Elasticsearch()

        query = {
            'sort' : [{ '@timestamp' : {'order' : 'asc'}}],
            'query' : {
                'term' : { 'jobid' : '9034906.0@fifebatch1.fnal.gov' }
            }
        }

        es_response= es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
        pprint.pprint(es_response)
        return template.render(pomspath=self.path, es_response=es_response)


####################
### UtilsPOMS

    @cherrypy.expose
    def quick_search(self, search_term):
        self.utilsPOMS.quick_search(cherrypy.request.db, cherrypy.HTTPRedirect, search_term)


    @cherrypy.expose
    def jump_to_job(self, jobsub_job_id, **kwargs):
        self.utilsPOMS.jump_to_job(cherrypy.request.db, cherrypy.HTTPRedirect, jobsub_job_id, **kwargs)
#----------------

##############################
### CALENDAR
#Using CalendarPOMS.py module
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def calendar_json(self, start, end, timezone, _):
        return self.calendarPOMS.calendar_json(cherrypy.request.db, start, end, timezone, _)


    @cherrypy.expose
    def calendar(self):
        template = self.jinja_env.get_template('calendar.html')
        rows = self.calendarPOMS.calendar(dbhandle = cherrypy.request.db)
        return template.render(rows=rows,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="CalendarHelp")


    @cherrypy.expose
    def add_event(self, title, start, end):
        #title should be something like minos_sam:27 DCache:12 All:11 ...
        return self.calendarPOMS.add_event.calendar(cherrypy.request.db,title, start, end)

    @cherrypy.expose
    def edit_event(self, title, start, new_start, end, s_id):  #even though we pass in the s_id we should not rely on it because they can and will change the service name
        return self.calendarPOMS.edit_event(cherrypy.request.db, title, start, new_start, end, s_id)


    @cherrypy.expose
    def service_downtimes(self):
        template = self.jinja_env.get_template('service_downtimes.html')
        rows = self.calendarPOMS.service_downtimes(cherrypy.request.db)
        return template.render(rows=rows,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="ServiceDowntimesHelp")


    @cherrypy.expose
    def update_service(self, name, parent, status, host_site, total, failed, description):
        return self.calendarPOMS.update_service(cherrypy.request.db, cherrypy.log, name, parent, status, host_site, total, failed, description)


    @cherrypy.expose
    def service_status(self, under='All'):
        template = self.jinja_env.get_template('service_status.html')
        list=self.calendarPOMS.service_status (cherrypy.request.db, under)
        return template.render(list=list,
                                name=under,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="ServiceStatusHelp",
                                version=self.version)
#--------------------------


######
    #print "Check where should be this function."
    '''
    Apparently this function is not related with Calendar
    '''
    def service_status_hier(self, under='All', depth=0):
        p = cherrypy.request.db.query(Service).filter(Service.name==under).first()
        if depth == 0:
            res = '<div class="ui accordion styled">\n'
        else:
            res = ''
        active = ""
        for s in cherrypy.request.db.query(Service).filter(Service.parent_service_id==p.service_id).order_by(Service.name).all():
             posneg = {"good": "positive", "degraded": "orange", "bad": "negative"}.get(s.status, "")
             icon = {"good": "checkmark", "bad": "remove", "degraded": "warning sign"}.get(s.status,"help circle")
             if s.host_site:
                 res = res + """
                     <div class="title %s">
                      <i class="dropdown icon"></i>
                      <button class="ui button %s tbox_delayed" data-content="%s" data-variation="basic">
                         %s (%d/%d)
                         <i class="icon %s"></i>
                       </button>
                     </div>
                     <div  class="content %s">
                         <a target="_blank" href="%s">
                         <i class="icon external"></i>
                         source webpage
                         </a>
                     </div>
                  """ % (active, posneg, s.description, s.name, s.failed_items, s.items, icon, active, s.host_site)
             else:
                 res = res + """
                    <div class="title %s">
                      <i class="dropdown icon"></i>
                      <button class="ui button %s tbox_delayed" data-content="%s" data-variation="basic">
                       %s (%d/%d)
                      <i class="icon %s"></i>
                      </button>
                    </div>
                    <div class="content %s">
                      <p>components:</p>
                      %s
                    </div>
                 """ % (active, posneg, s.description, s.name, s.failed_items, s.items, icon, active,  self.service_status_hier(s.name, depth + 1))
             active = ""

        if depth == 0:
            res = res + "</div>"
        return res
#####
### DBadminPOMS
    @cherrypy.expose
    def raw_tables(self):
        if not self.accessPOMS.can_db_admin(cherrypy.request.headers.get, cherrypy.session.get):
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('raw_tables.html')
        return template.render(list=self.tablesPOMS.admin_map.keys(), current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="RawTablesHelp", version=self.version)


    @cherrypy.expose
    def user_edit(self, *args, **kwargs):
        data = self.dbadminPOMS.user_edit(cherrypy.request.db, *args, **kwargs)
        template = self.jinja_env.get_template('user_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path, help_page="EditUsersHelp", version=self.version)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def experiment_members(self, experiment, *args, **kwargs):
        trows = self.dbadminPOMS.experiment_members(cherrypy.request.db, experiment, *args, **kwargs)
        return trows


    @cherrypy.expose
#    @cherrypy.tools.json_out()
    def member_experiments(self, email, *args, **kwargs):
        trows = self.dbadminPOMS.member_experiments(cherrypy.request.db, email, *args, **kwargs)
        return trows


    @cherrypy.expose
    def experiment_edit(self, message=None):
        experiments=self.dbadminPOMS.experiment_edit(cherrypy.request.db)
        template = self.jinja_env.get_template('experiment_edit.html')
        return template.render(message=message, experiments=experiments, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="ExperimentEditHelp", version=self.version)


    @cherrypy.expose
    def experiment_authorize(self, *args, **kwargs):
        if not self.accessPOMS.can_db_admin(cherrypy.request.headers.get, cherrypy.session.get):
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        message = self.dbadminPOMS.experiment_authorize(cherrypy.request.db, cherrypy.log, *args, **kwargs)
        return self.experiment_edit(message)
#-----------------------------------------
#################################
### CampaignsPOMS
    @cherrypy.expose
    def launch_template_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.launch_template_edit(cherrypy.request.db, cherrypy.log, cherrypy.session.get, *args, **kwargs)
        template = self.jinja_env.get_template('launch_template_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="LaunchTemplateEditHelp", version=self.version)


    @cherrypy.expose
    def campaign_definition_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_definition_edit(cherrypy.request.db, cherrypy.log, cherrypy.session.get, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_definition_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="CampaignDefinitionEditHelp", version=self.version)


    @cherrypy.expose
    def campaign_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit(cherrypy.request.db, cherrypy.log, cherrypy.session, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="CampaignEditHelp", version=self.version)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def campaign_edit_query(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit_query(cherrypy.request.db, *args, **kwargs)
        return data


    @cherrypy.expose
    def new_task_for_campaign(self, campaign_name, command_executed, experimenter_name, dataset_name = None):
        return self.campaignsPOMS.new_task_for_campaign(cherrypy.request.db, campaign_name, command_executed, experimenter_name, dataset_name)


    @cherrypy.expose
    def show_campaigns(self, experiment=None, tmin=None, tmax=None, tdays=1, active=True, **kwargs):
        (counts, counts_keys, clist, dimlist,
            tmin, tmax, tmins, tmaxs,
            nextlink, prevlink, time_range_string
        ) = self.campaignsPOMS.show_campaigns(cherrypy.request.db, cherrypy.log,
                                            cherrypy.request.samweb_lite, experiment=experiment,
                                            tmin=tmin, tmax=tmax, tdays=tdays, active=active)

        current_experimenter = cherrypy.session.get('experimenter')
        #~ cherrypy.log("current_experimenter.extra before: "+str(current_experimenter.extra))     # DEBUG
        if 'exp_selected' in kwargs:
            current_experimenter.extra = {'selected': kwargs['exp_selected']}
            cherrypy.session['experimenter'] = current_experimenter
            #~ cherrypy.log("current_experimenter.extra update... ")                               # DEBUG
        #~ cherrypy.log("current_experimenter.extra after: "+str(current_experimenter.extra))      # DEBUG

        experiments = self.dbadminPOMS.member_experiments(cherrypy.request.db, current_experimenter.email)

        template = self.jinja_env.get_template('show_campaigns.html')

        return template.render(In=("" if active == True else "In"), limit_experiment=experiment,
                                services=self.service_status_hier('All'), counts=counts, counts_keys=counts_keys,
                                cl=clist, tmins=tmins, tmaxs=tmaxs, tmin=str(tmin)[:16], tmax=str(tmax)[:16],
                                current_experimenter=current_experimenter, do_refresh=1,
                                next=nextlink, prev=prevlink, days=tdays, time_range_string=time_range_string,
                                key='', dimlist=dimlist, pomspath=self.path, help_page="ShowCampaignsHelp",
                                experiments=experiments,
                                dbg=kwargs,
                                version=self.version)


    @cherrypy.expose
    def campaign_info(self, campaign_id, tmin=None, tmax=None, tdays=None):
        (Campaign_info, time_range_string, tmins, tmaxs, Campaign_definition_info,
            Launch_template_info, tags, launched_campaigns, dimlist, cl, counts_keys,
            counts, launch_flist) = self.campaignsPOMS.campaign_info(cherrypy.request.db, cherrypy.log,
                                        cherrypy.request.samweb_lite, cherrypy.HTTPError, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_info.html')
        return template.render(Campaign_info=Campaign_info, time_range_string=time_range_string, tmins=tmins, tmaxs=tmaxs,
                        Campaign_definition_info=Campaign_definition_info, Launch_template_info=Launch_template_info,
                        tags=tags, launched_campaigns=launched_campaigns, dimlist=dimlist,
                        cl=cl, counts_keys=counts_keys, counts=counts, launch_flist=launch_flist,
                        current_experimenter=cherrypy.session.get('experimenter'),
                        do_refresh=0, pomspath=self.path,help_page="CampaignInfoHelp", version=self.version)


    @cherrypy.expose
    def campaign_time_bars(self, campaign_id=None, tag=None, tmin=None, tmax=None, tdays=1):
        (job_counts, blob, name, tmin, tmax,
            nextlink, prevlink, tdays,key, extramap) = self.campaignsPOMS.campaign_time_bars(cherrypy.request.db,
                                                            campaign_id = campaign_id, tag=tag, tmin=tmin, tmax=tmax, tdays=tdays)
        template = self.jinja_env.get_template('campaign_time_bars.html')
        return template.render(job_counts=job_counts, blob=blob, name=name, tmin=tmin, tmax=tmax,
                    current_experimenter=cherrypy.session.get('experimenter'),
                    do_refresh=1, next=nextlink, prev=prevlink, days=tdays, key=key,
                    pomspath=self.path, extramap=extramap, help_page="CampaignTimeBarsHelp", version=self.version)


    @cherrypy.expose
    def register_poms_campaign(self, experiment, campaign_name, version, user=None, campaign_definition=None, dataset="", role="Analysis", params=[]):
        campaign_id = self.campaignsPOMS.register_poms_campaign(cherrypy.request.db, cherrypy.log,
                            experiment, campaign_name, version, user, campaign_definition, dataset, role, params)
        return "Campaign=%d" % campaign_id


    @cherrypy.expose
    def list_launch_file(self, campaign_id, fname):
        lines = self.campaignsPOMS.list_launch_file(campaign_id, fname)
        output = "".join(lines)
        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command='', output=output,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                c=None, campaign_id=campaign_id, pomspath=self.path,
                                help_page="LaunchedJobsHelp", version=self.version)
        return res


    @cherrypy.expose
    def schedule_launch(self, campaign_id):
        c, job, launch_flist = self.campaignsPOMS.schedule_launch(cherrypy.request.db, campaign_id)
        template = self.jinja_env.get_template('schedule_launch.html')
        return template.render(c=c, campaign_id=campaign_id, job=job,
                        current_experimenter=cherrypy.session.get('experimenter'),
                        do_refresh=0, pomspath=self.path, help_page="ScheduleLaunchHelp",
                        launch_flist=launch_flist, version=self.version)


    @cherrypy.expose
    def update_launch_schedule(self, campaign_id, dowlist=None, domlist=None, monthly=None, month=None, hourlist=None, submit=None, minlist=None, delete=None):
        self.campaignsPOMS.update_launch_schedule(cherrypy.log, campaign_id, dowlist, domlist, monthly, month, hourlist, submit, minlist, delete)
        raise cherrypy.HTTPRedirect("schedule_launch?campaign_id=%s" % campaign_id)


    @cherrypy.expose
    def mark_campaign_active(self, campaign_id, is_active):
        c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        if c and (cherrypy.session.get('experimenter').is_authorized(c.experiment) or self.accessPOMS.can_report_data( cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get )()):
            c.active=(is_active == 'True')
            cherrypy.request.db.add(c)
            cherrypy.request.db.commit()
            raise cherrypy.HTTPRedirect("campaign_info?campaign_id=%s" % campaign_id)
        else:
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')


    @cherrypy.expose
    def make_stale_campaigns_inactive(self):
        if not self.accessPOMS.can_report_data(cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get):
             raise err_res(401, 'You are not authorized to access this resource')
        res = self.campaignsPOMS.make_stale_campaigns_inactive(cherrypy.request.db, cherrypy.HTTPError)
        return "Marked inactive stale: " + ",".join(res)
#--------------------------------------
###############
### Tables
    @cherrypy.expose
    def list_generic(self, classname):
        l = self.tablesPOMS.list_generic(cherrypy.request.db, cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session.get, classname)
        template = self.jinja_env.get_template('list_generic.html')
        return template.render(classname=classname,
                        list=l, edit_screen="edit_screen_generic",
                        primary_key='experimenter_id',
                        current_experimenter=cherrypy.session.get('experimenter'),
                        pomspath=self.path, help_page="ListGenericHelp", version=self.version)


    @cherrypy.expose
    def edit_screen_generic(self, classname, id=None):
        return self.tablesPOMS.edit_screen_generic(cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session.get, classname, id)


    @cherrypy.expose
    def update_generic(self, classname, *args, **kwargs):
        return self.tablesPOMS.update_generic(cherrypy.request.db, cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get, classname, *args, **kwargs)


    def edit_screen_for(self, classname, eclass, update_call, primkey, primval, valmap): ### Why this function is not expose
        screendata = self.tablesPOMS.edit_screen_for(cherrypy.request.db,
                            cherrypy.log, cherrypy.request.headers.get,
                            cherrypy.session.get, classname, eclass,
                            update_call, primkey, primval, valmap)
        template = self.jinja_env.get_template('edit_screen_for.html')
        return template.render(screendata=screendata, action="./"+update_call,
                        classname=classname,
                        current_experimenter=cherrypy.session.get('experimenter'),
                        pomspath=self.path, help_page="GenericEditHelp", version=self.version)
#----------------------------
#######
### JobPOMS
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def active_jobs(self):
        res = self.jobsPOMS.active_jobs(cherrypy.request.db)
        return res


    @cherrypy.expose
    def report_declared_files(self, flist):
        self.filesPOMS.report_declared_files(flist, cherrypy.request.db)
        return "Ok."


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def output_pending_jobs(self):
        res = self.jobsPOMS.output_pending_jobs(cherrypy.request.db)
        return res


    @cherrypy.expose
    def update_job(self, task_id, jobsub_job_id,  **kwargs):
        cherrypy.log("update_job( task_id %s, jobsub_job_id %s,  kwargs %s )" % (task_id, jobsub_job_id, repr(kwargs)))
        if not self.accessPOMS.can_report_data( cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get ):
            cherrypy.log("update_job: not allowed")
            return "Not Allowed"
        return (self.jobsPOMS.update_job(cherrypy.request.db, cherrypy.log, cherrypy.response.status, cherrypy.request.samweb_lite, task_id, jobsub_job_id, **kwargs))


    @cherrypy.expose
    def test_job_counts(self, task_id=None, campaign_id=None):
        res = self.triagePOMS.job_counts(cherrypy.request.db, task_id, campaign_id)
        return repr(res) + self.filesPOMS.format_job_counts(task_id, campaign_id)


    @cherrypy.expose
    def kill_jobs(self, campaign_id=None, task_id=None, job_id=None, confirm=None):
        if confirm == None:
            jjil, t, campaign_id, task_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, cherrypy.log, campaign_id, task_id, job_id, confirm)
            template = self.jinja_env.get_template('kill_jobs_confirm.html')
            return template.render(current_experimenter=cherrypy.session.get('experimenter'),
                                    jjil=jjil, task=t, campaign_id=campaign_id,
                                    task_id=task_id, job_id=job_id, pomspath=self.path,
                                    help_page="KilledJobsHelp", version=self.version)
        else:
            output, c, campaign_id, task_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, cherrypy.log, campaign_id, task_id, job_id, confirm)
            template = self.jinja_env.get_template('kill_jobs.html')
            return template.render(output=output, current_experimenter=cherrypy.session.get('experimenter'),
                                    c=c, campaign_id=campaign_id, task_id=task_id,
                                    job_id=job_id, pomspath=self.path,
                                    help_page="KilledJobsHelp", version=self.version)


    @cherrypy.expose
    def jobs_eff_histo(self, campaign_id, tmax=None, tmin=None, tdays=1):
        (c, maxv, total, vals,
            tmaxs, campaign_id,
            tdays, tmin, tmax,
            nextlink, prevlink, tdays) = self.jobsPOMS.jobs_eff_histo(cherrypy.request.db, campaign_id, tmax, tmin, tdays)
        template = self.jinja_env.get_template('jobs_eff_histo.html')
        return template.render(c=c, maxv=maxv, total=total,
                                vals=vals, tmaxs=tmaxs,
                                campaign_id=campaign_id,
                                tdays=tdays, tmin=tmin, tmax=tmax,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=1, next=nextlink, prev=prevlink,
                                days=tdays, pomspath=self.path,
                                help_page="JobEfficiencyHistoHelp", version=self.version)


    @cherrypy.expose
    def set_job_launches(self, hold):
        self.taskPOMS.set_job_launches(cherrypy.request.db, hold)
        raise cherrypy.HTTPRedirect(self.path + "/")

    @cherrypy.expose
    def launch_queued_job(self):
        return self.taskPOMS.launch_queued_job(cherrypy.request.db,cherrypy.log, cherrypy.session.get, cherrypy.request.headers.get, cherrypy.session.get, cherrypy.response.status)

    @cherrypy.expose
    def launch_jobs(self, campaign_id, dataset_override=None, parent_task_id=None): ###needs to be analize in detail.
        vals = self.taskPOMS.launch_jobs(cherrypy.request.db,
                        cherrypy.log, cherrypy.session.get,
                        cherrypy.request.headers.get, cherrypy.session.get,
                        cherrypy.request.samweb_lite,
                        cherrypy.response.status, campaign_id, dataset_override, parent_task_id)
        cherrypy.log("Got vals: %s" % repr(vals))
        lcmd, output, c, campaign_id, outdir, outfile = vals
        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command=lcmd, output=output,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                c=c, campaign_id=campaign_id, pomspath=self.path,
                                help_page="LaunchedJobsHelp", version=self.version)
        return res
#----------------------
########################
### TaskPOMS
    @cherrypy.expose
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor):
         if not can_create_task():
             return "Not Allowed"
         return (self.taskPOMS.create_task(cherrypy.request.db, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor))

    @cherrypy.expose
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return "\n".join(self.taskPOMS.wrapup_tasks(cherrypy.request.db, cherrypy.log, cherrypy.request.samweb_lite, cherrypy.config.get, cherrypy.request.headers.get, cherrypy.session.get, cherrypy.response.status))

    @cherrypy.expose
    def show_task_jobs(self, task_id, tmax=None, tmin=None, tdays=1): ### Need to be tested HERE
        (blob, job_counts,
            task_id, tmin, tmax,
            extramap, key, task_jobsub_id,
            campaign_id, cname) = self.taskPOMS.show_task_jobs( cherrypy.request.db, task_id, tmax, tmin, tdays)
        template = self.jinja_env.get_template('show_task_jobs.html')
        return template.render(blob=blob, job_counts=job_counts, taskid=task_id, tmin=tmin, tmax=tmax,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                extramap=extramap, do_refresh=1, key=key, pomspath=self.path, help_page="ShowTaskJobsHelp",
                                task_jobsub_id=task_jobsub_id,
                                campaign_id=campaign_id, cname=cname, version=self.version)


    @cherrypy.expose
    def get_task_id_for(self, campaign, user=None, experiment=None, command_executed="", input_dataset="", parent_task_id=None):
        task_id = self.taskPOMS.get_task_id_for(cherrypy.request.db, campaign, user, experiment, command_executed, input_dataset, parent_task_id)
        return "Task=%d" % task_id
#------------------------
#########################
### FilesPOMS
    @cherrypy.expose
    def list_task_logged_files(self, task_id):
        fl, t, jobsub_job_id = self.filesPOMS.list_task_logged_files(cherrypy.request.db, task_id)
        template = self.jinja_env.get_template('list_task_logged_files.html')
        return template.render(fl=fl, campaign=t.campaign_snap_obj, jobsub_job_id=jobsub_job_id,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0, pomspath=self.path,
                                help_page="ListTaskLoggedFilesHelp", version=self.version)


    @cherrypy.expose
    def campaign_task_files(self, campaign_id, tmin=None, tmax=None, tdays=1):
        (c, columns, datarows,
            tmins, tmaxs,
            prevlink, nextlink, tdays) = self.filesPOMS.campaign_task_files(cherrypy.request.db, cherrypy.log, cherrypy.request.samweb_lite, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_task_files.html')
        return template.render(name = c.name if c else "",
                                columns=columns, datarows=datarows,
                                tmin=tmins, tmax=tmaxs,
                                prev=prevlink, next=nextlink, days=tdays,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                campaign_id=campaign_id, pomspath=self.path, help_page="CampaignTaskFilesHelp", version=self.version)


    @cherrypy.expose
    def job_file_list(self, job_id,force_reload = False): ##Ask Marc to check this in the module
        return self.filesPOMS.job_file_list(cherrypy.request.db, cherrypy.request.jobsub_fetcher, job_id, force_reload)


    @cherrypy.expose
    def job_file_contents(self, job_id, task_id, file, tmin=None, tmax=None, tdays=None):
        job_file_contents, tmin = self.filesPOMS.job_file_contents(cherrypy.request.db, cherrypy.log,
                                            cherrypy.request.jobsub_fetcher, job_id, task_id, file, tmin, tmax, tdays)
        template = self.jinja_env.get_template('job_file_contents.html')
        return template.render(file=file, job_file_contents=job_file_contents,
                                task_id=task_id, job_id=job_id, tmin=tmin,
                                pomspath=self.path, help_page="JobFileContentsHelp", version=self.version)


    @cherrypy.expose
    def inflight_files(self, campaign_id=None, task_id=None):
        outlist, statusmap, c = self.filesPOMS.inflight_files(cherrypy.request.db, cherrypy.response.status, cherrypy.config.get, campaign_id, task_id)
        template = self.jinja_env.get_template('inflight_files.html')
        return template.render(flist=outlist,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                statusmap=statusmap, c=c,
                                jjid=self.taskPOMS.task_min_job(cherrypy.request.db, task_id),
                                campaign_id=campaign_id, task_id=task_id,
                                pomspath=self.path, help_page="PendingFilesJobsHelp", version=self.version)


    @cherrypy.expose
    def show_dimension_files(self, experiment, dims):
        flist = self.filesPOMS.show_dimension_files(cherrypy.request.samweb_lite, experiment, dims)
        template = self.jinja_env.get_template('show_dimension_files.html')
        return template.render(flist=flist, dims=dims,
                                current_experimenter=cherrypy.session.get('experimenter'), statusmap=[],
                                pomspath=self.path, help_page="ShowDimensionFilesHelp", version=self.version)


    @cherrypy.expose
    def actual_pending_files(self, count_or_list, task_id=None, campaign_id=None, tmin=None, tmax=None, tdays=1): ###??? Implementation of the exception.
        cherrypy.response.timeout = 600
        try:
            c.experiment, dims = self.filesPOMS.actual_pending_files(cherrypy.request.db, cherrypy.log, count_or_list, task_id, campaign_id, tmin, tmax, tdays)
            return self.show_dimension_files(c.experiment, dims)
        except ValueError:
            return "None == dims in actual_pending_files method"


    @cherrypy.expose
    def campaign_sheet(self, campaign_id, tmin=None, tmax=None , tdays=7):
        (name, columns, outrows, dimlist,
            experiment, tmaxs,
            prevlink, nextlink,
            tdays, tmin, tmax) = self.filesPOMS.campaign_sheet(cherrypy.request.db, cherrypy.log, cherrypy.request.samweb_lite, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_sheet.html')
        return template.render(name=name,
                                columns=columns,
                                datarows=outrows,
                                dimlist=dimlist,
                                tmaxs=tmaxs,
                                prev=prevlink,
                                next=nextlink,
                                days=tdays,
                                tmin=tmin,
                                tmax=tmax,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                campaign_id=campaign_id,
                                experiment=experiment,
                                pomspath=self.path,
                                help_page="CampaignSheetHelp",
                                version=self.version)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def json_project_summary_for_task(self, task_id):
        return self.project_summary_for_task(task_id)


    def project_summary_for_task(self, task_id):
        t = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).first()
        return cherrypy.request.samweb_lite.fetch_info(t.campaign_snap_obj.experiment, t.project)


    def project_summary_for_tasks(self, task_list):
        return cherrypy.request.samweb_lite.fetch_info_list(task_list)
        #~ return [ {"tot_consumed": 0, "tot_failed": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug
###Im here
#----------------------------
##########
### TriagePOMS
    @cherrypy.expose
    def triage_job(self, job_id, tmin=None, tmax=None, tdays=None, force_reload=False):
        (job_file_list, job_info, job_history,
            downtimes, output_file_names_list,
            es_response, efficiency,
            tmin, task_jobsub_job_id) = self.triagePOMS.triage_job(cherrypy.request.db, cherrypy.request.jobsub_fetcher, cherrypy.config, job_id, tmin, tmax, tdays, force_reload)
        template = self.jinja_env.get_template('triage_job.html')
        return template.render(job_id=job_id,
                                job_file_list=job_file_list,
                                job_info=job_info,
                                job_history=job_history,
                                downtimes=downtimes,
                                output_file_names_list=output_file_names_list,
                                es_response=es_response,
                                efficiency=efficiency,
                                tmin=tmin,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="TriageJobHelp",
                                task_jobsub_job_id=task_jobsub_job_id,
                                version=self.version)


    @cherrypy.expose
    def job_table(self, offset=0, **kwargs):
        ###The pass of the arguments is ugly we will fix that later.
        (jl, jobcolumns, taskcolumns,
            campcolumns, tmins, tmaxs,
            prevlink, nextlink, tdays,
            extra, hidecolumns, filtered_fields,
            time_range_string) = self.triagePOMS.job_table(cherrypy.request.db, **kwargs)

        template = self.jinja_env.get_template('job_table.html')

        return template.render(joblist=jl,
                                jobcolumns=jobcolumns,
                                taskcolumns=taskcolumns,
                                campcolumns=campcolumns,
                                tmin=tmins,
                                tmax=tmaxs,
                                prev=prevlink,
                                next=nextlink,
                                days=tdays,
                                extra=extra,
                                hidecolumns=hidecolumns,
                                filtered_fields=filtered_fields,
                                time_range_string=time_range_string,
                                offset=int(offset),
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0,
                                pomspath=self.path,
                                help_page="JobTableHelp",
                                version=self.version)


    @cherrypy.expose
    def jobs_by_exitcode(self, tmin = None, tmax =  None, tdays = 1 ):
        raise cherrypy.HTTPRedirect("%s/failed_jobs_by_whatever?f=user_exe_exit_code&tdays=%s" % (self.path, tdays))


    @cherrypy.expose
    def failed_jobs_by_whatever(self, tmin=None, tmax=None, tdays=1 , f=[], go=None):
        (jl, possible_columns, columns,
            tmins, tmaxs, tdays,
            prevlink, nextlink,
            time_range_string, tdays) = self.triagePOMS.failed_jobs_by_whatever(cherrypy.request.db, cherrypy.log, tmin, tmax, tdays, f, go)
        template = self.jinja_env.get_template('failed_jobs_by_whatever.html')
        return template.render(joblist=jl,
                                possible_columns=possible_columns,
                                columns=columns,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0,
                                tmin=tmins,
                                tmax=tmaxs,
                                tdays=tdays,
                                prev=prevlink,
                                next=nextlink,
                                time_range_string=time_range_string,
                                days=tdays,
                                pomspath=self.path,
                                help_page="JobsByExitcodeHelp",
                                version=self.version)
#-------------------
##############
### TagsPOMS
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def link_tags(self, campaign_id, tag_name, experiment):
        return(self.tagsPOMS.link_tags(cherrypy.request.db, cherrypy.session.get, campaign_id, tag_name, experiment))


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_campaigns_tags(self, campaign_id, tag_id, experiment):
        return(self.tagsPOMS.delete_campaigns_tags( cherrypy.request.db, campaign_id, tag_id, experiment))


    @cherrypy.expose
    def search_tags(self, q):
        results, q_list = self.tagsPOMS.search_tags(cherrypy.request.db, q)
        template = self.jinja_env.get_template('search_tags.html')
        return template.render(results=results,
                                q_list=q_list,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0,
                                pomspath=self.path,
                                help_page="SearchTagsHelp",
                                version=self.version)


    @cherrypy.expose
    def auto_complete_tags_search(self, experiment, q):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return(self.tagsPOMS.auto_complete_tags_search(cherrypy.request.db, experiment, q))
#-----------------------
