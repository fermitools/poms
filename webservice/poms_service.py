import os
import pprint
import socket

import cherrypy
from jinja2 import Environment, PackageLoader
from collections import deque
from . import CalendarPOMS
from . import CampaignsPOMS
from . import DBadminPOMS
from . import FilesPOMS
from . import JobsPOMS
from . import TablesPOMS
from . import TagsPOMS
from . import TaskPOMS
from . import TriagePOMS
from . import UtilsPOMS
from . import logit
from . import version
from .elasticsearch import Elasticsearch
from .poms_model import Service, Task, Campaign


#import gcwrap


def error_response():
    dump = ""
    if cherrypy.config.get("dump", True):
        dump = cherrypy._cperror.format_exc()
    message = dump.replace('\n', '<br/>')

    jinja_env = Environment(loader=PackageLoader('poms.webservice', 'templates'))
    template = jinja_env.get_template('error_response.html')
    path = cherrypy.config.get("pomspath", "/poms")
    body = template.render(message=message, pomspath=path, dump=dump, version=global_version)
    cherrypy.response.status = 500
    cherrypy.response.headers['content-type'] = 'text/html'
    cherrypy.response.body = body.encode()
    logit.log(dump)



class PomsService(object):


    _cp_config = {'request.error_response': error_response,
                  'error_page.404': "%s/%s" % (os.path.abspath(os.getcwd()), '/templates/page_not_found.html')
                 }

    def __init__(self):
        ##
        ##  USE post_initialize if you need to log data!!!
        ##
        global global_version
        self.jinja_env = Environment(loader=PackageLoader('poms.webservice', 'templates'))
        self.path = cherrypy.config.get("pomspath", "/poms")
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
        self.triagePOMS = TriagePOMS.TriagePOMS(self)
        self.tablesPOMS = None

    def post_initialize(self):
        # Anything that needs to log data must be called here -- after loggers are configured.
        self.tablesPOMS = TablesPOMS.TablesPOMS(self)


    @cherrypy.expose
    @logit.logstartstop
    def headers(self):
        return repr(cherrypy.request.headers)


    @cherrypy.expose
    @logit.logstartstop
    def sign_out(self):
        cherrypy.lib.sessions.expire()
        log_out_url = "https://" + self.hostname + "/Shibboleth.sso/Logout"
        raise cherrypy.HTTPRedirect(log_out_url)


    @cherrypy.expose
    @logit.logstartstop
    def index(self):
        template = self.jinja_env.get_template('index.html')
        return template.render(services=self.service_status_hier('All'),
                               launches=self.taskPOMS.get_job_launches(cherrypy.request.db),
                               do_refresh=1200, help_page="DashboardHelp")


    @cherrypy.expose
    @logit.logstartstop
    def es(self):
        template = self.jinja_env.get_template('elasticsearch.html')

        es = Elasticsearch(config=cherrypy.config)

        query = {
            'sort': [{'@timestamp': {'order': 'asc'}}],
            'query': {
                'term': {'jobid': '17519748.0@fifebatch2.fnal.gov'}
            }
        }

        es_response = es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
        pprint.pprint(es_response)
        return template.render(es_response=es_response)


####################
### UtilsPOMS

    @cherrypy.expose
    @logit.logstartstop
    def quick_search(self, search_term):
        self.utilsPOMS.quick_search(cherrypy.request.db, cherrypy.HTTPRedirect, search_term)


    @cherrypy.expose
    @logit.logstartstop
    def jump_to_job(self, jobsub_job_id, **kwargs):
        self.utilsPOMS.jump_to_job(cherrypy.request.db, cherrypy.HTTPRedirect, jobsub_job_id, **kwargs)


    @cherrypy.expose
    @logit.logstartstop
    def update_session_experiment(self, *args, **kwargs):
        self.utilsPOMS.update_session_experiment(cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

##############################
### CALENDAR
#Using CalendarPOMS.py module
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def calendar_json(self, start, end, timezone, _):
        return list(self.calendarPOMS.calendar_json(cherrypy.request.db, start, end, timezone, _))


    @cherrypy.expose
    @logit.logstartstop
    def calendar(self):
        template = self.jinja_env.get_template('calendar.html')
        rows = self.calendarPOMS.calendar(dbhandle=cherrypy.request.db)
        return template.render(rows=rows, help_page="CalendarHelp")


    @cherrypy.expose
    @logit.logstartstop
    def add_event(self, title, start, end):
        #title should be something like minos_sam:27 DCache:12 All:11 ...
        return self.calendarPOMS.add_event.calendar(cherrypy.request.db, title, start, end)

    @cherrypy.expose
    @logit.logstartstop
    def edit_event(self, title, start, new_start, end, s_id):
        # even though we pass in the s_id we should not rely on it because they can and will change the service name
        return self.calendarPOMS.edit_event(cherrypy.request.db, title, start, new_start, end, s_id)


    @cherrypy.expose
    @logit.logstartstop
    def service_downtimes(self):
        template = self.jinja_env.get_template('service_downtimes.html')
        rows = self.calendarPOMS.service_downtimes(cherrypy.request.db)
        return template.render(rows=rows, help_page="ServiceDowntimesHelp")


    @cherrypy.expose
    @logit.logstartstop
    def update_service(self, name, parent, status, host_site, total, failed, description):
        return self.calendarPOMS.update_service(cherrypy.request.db, name, parent, status, host_site, total, failed, description)


    @cherrypy.expose
    @logit.logstartstop
    def service_status(self, under='All'):
        template = self.jinja_env.get_template('service_status.html')
        list_ = self.calendarPOMS.service_status(cherrypy.request.db, under)
        return template.render(list=list_, name=under, help_page="ServiceStatusHelp")

######
    #print "Check where should be this function."
    '''
    Apparently this function is not related with Calendar
    '''
    def service_status_hier(self, under='All', depth=0):
        p = cherrypy.request.db.query(Service).filter(Service.name == under).first()
        if depth == 0:
            res = '<div class="ui accordion styled">\n'
        else:
            res = ''
        active = ""

        for s in cherrypy.request.db.query(Service).filter(Service.parent_service_id == p.service_id).order_by(Service.name).all():
            posneg = {"good": "positive", "degraded": "orange", "bad": "negative"}.get(s.status, "")
            icon = {"good": "checkmark", "bad": "remove", "degraded": "warning sign"}.get(s.status, "help circle")
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
                """ % (active, posneg, s.description, s.name, s.failed_items, s.items, icon, active,
                       self.service_status_hier(s.name, depth + 1))
            active = ""

        if depth == 0:
            res = res + "</div>"
        return res


#####
### DBadminPOMS
    @cherrypy.expose
    @logit.logstartstop
    def raw_tables(self):
        if not cherrypy.session.get('experimenter').is_root():
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('raw_tables.html')
        #print("*" * 80)
        #print("*" * 80)
        #print("%s" % str(list(self.tablesPOMS.admin_map.keys())))
        #print("*" * 80)
        #print("*" * 80)
        return template.render(tlist=list(self.tablesPOMS.admin_map.keys()), help_page="RawTablesHelp")


    @cherrypy.expose
    @logit.logstartstop
    def user_edit(self, *args, **kwargs):
        data = self.dbadminPOMS.user_edit(cherrypy.request.db, *args, **kwargs)
        template = self.jinja_env.get_template('user_edit.html')
        return template.render(data=data, help_page="EditUsersHelp")


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def experiment_members(self, experiment, *args, **kwargs):
        trows = self.dbadminPOMS.experiment_members(cherrypy.request.db, experiment, *args, **kwargs)
        return trows


    @cherrypy.expose
    @logit.logstartstop
    def member_experiments(self, username, *args, **kwargs):
        trows = self.dbadminPOMS.member_experiments(cherrypy.request.db, username, *args, **kwargs)
        return trows


    @cherrypy.expose
    @logit.logstartstop
    def experiment_edit(self, message=None):
        experiments = self.dbadminPOMS.experiment_edit(cherrypy.request.db)
        template = self.jinja_env.get_template('experiment_edit.html')
        return template.render(message=message, experiments=experiments, help_page="ExperimentEditHelp")


    @cherrypy.expose
    @logit.logstartstop
    def experiment_authorize(self, *args, **kwargs):
        if not cherrypy.session.get('experimenter').is_root():
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        message = self.dbadminPOMS.experiment_authorize(cherrypy.request.db, *args, **kwargs)
        return self.experiment_edit(message)
#-----------------------------------------
#################################
### CampaignsPOMS

    @cherrypy.expose
    @logit.logstartstop
    def launch_template_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.launch_template_edit(cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

        if kwargs.get('test_template'):
            
             raise cherrypy.HTTPRedirect("%s/launch_jobs?campaign_id=None&test_launch_template=%s"  % (self.path, kwargs.get('ae_launch_id')))

        template = self.jinja_env.get_template('launch_template_edit.html')
        return template.render(data=data, help_page="LaunchTemplateEditHelp")


    @cherrypy.expose
    def campaign_deps(self, tag = None, camp_id = None):
        template = self.jinja_env.get_template('campaign_deps.html')
        svgdata = self.campaignsPOMS.campaign_deps_svg(cherrypy.request.db, cherrypy.config.get, tag, camp_id)
        return template.render(tag=tag, svgdata=svgdata, help_page="CampaignDepsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def campaign_definition_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_definition_edit(cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

        if kwargs.get('test_template'):
             test_campaign = self.campaignsPOMS.make_test_campaign_for(cherrypy.request.db, cherrypy.session, kwargs.get("ae_campaign_definition_id"), kwargs.get("ae_definition_name"))
             raise cherrypy.HTTPRedirect("%s/campaign_edit?jump_to_campaign=%d&extra_edit_flag=launch_test_job"  % (self.path, test_campaign))

        template = self.jinja_env.get_template('campaign_definition_edit.html')
        return template.render(data=data, help_page="CampaignDefinitionEditHelp")

    @cherrypy.expose
    @logit.logstartstop
    def make_test_campaign_for(self,  campaign_def_id, campaign_def_name):
         cid = make_test_campaign_for(self, cherrypy.request.db, cherrypy.session, campaign_def_id, campaign_def_name)
         raise cherrypy.HTTPRedirect("%s/campaign_edit?campaign_id=%d&extra_edit_flag=launch_test_job" % (self.path, cid))
         

    @cherrypy.expose
    @logit.logstartstop
    def campaign_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit(cherrypy.request.db, cherrypy.session, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_edit.html')

        if kwargs.get('pcl_call', '0') == '1':
            if data['message']:
                raise cherrypy.HTTPError(400, data['message'])

        if kwargs.get('launch_test_job', None) and kwargs.get('ae_campaign_id', None):
           raise cherrypy.HTTPRedirect("%s/launch_jobs?campaign_id=%s" % (self.path, kwargs.get('ae_campaign_id')))

        return template.render(data=data, help_page="CampaignEditHelp", 
                               extra_edit_flag = kwargs.get("extra_edit_flag",None),
                               jump_to_campaign = kwargs.get("jump_to_campaign",None)
                        )

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_list_json(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_list(cherrypy.request.db)
        return data

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_edit_query(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit_query(cherrypy.request.db, *args, **kwargs)
        return data


    @cherrypy.expose
    @logit.logstartstop
    def new_task_for_campaign(self, campaign_name, command_executed, experimenter_name, dataset_name=None):
        return self.campaignsPOMS.new_task_for_campaign(cherrypy.request.db, campaign_name, command_executed,
                                                        experimenter_name, dataset_name)

    @cherrypy.expose
    @logit.logstartstop
    def show_tags(self):
        experiment = cherrypy.session.get('experimenter').session_experiment

        tl = self.tagsPOMS.show_tags(cherrypy.request.db, experiment)

        current_experimenter = cherrypy.session.get('experimenter')

        experiments = self.dbadminPOMS.member_experiments(cherrypy.request.db, current_experimenter.username)


        template = self.jinja_env.get_template('show_tags.html')
        return template.render(tl=tl, help_page="ShowCampaignTagsHelp", experiments=experiments)

    @cherrypy.expose
    @logit.logstartstop
    def show_campaigns(self, tmin=None, tmax=None, tdays=7, active=True, tag=None, cl=None, **kwargs):
        experiment = cherrypy.session.get('experimenter').session_experiment
        (
         campaigns, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string
        ) = self.campaignsPOMS.show_campaigns(cherrypy.request.db,
                                              cherrypy.request.samweb_lite, experiment=experiment,
                                              tmin=tmin, tmax=tmax, tdays=tdays, active=active, tag=tag,
                                              campaign_ids=cl)

        current_experimenter = cherrypy.session.get('experimenter')
        #~ logit.log("current_experimenter.extra before: "+str(current_experimenter.extra))     # DEBUG
        if 'exp_selected' in kwargs:
            current_experimenter.extra = {'selected': kwargs['exp_selected']}
            cherrypy.sesssion.acquire_lock()
            cherrypy.session['experimenter'] = current_experimenter
            cherrypy.session.save()
            cherrypy.sesssion.release_lock()
            #~ logit.log("current_experimenter.extra update... ")                               # DEBUG
        #~ logit.log("current_experimenter.extra after: "+str(current_experimenter.extra))      # DEBUG

        experiments = self.dbadminPOMS.member_experiments(cherrypy.request.db, current_experimenter.username)

        if cl is None:
            template = self.jinja_env.get_template('show_campaigns.html')
        else:
            template = self.jinja_env.get_template('show_campaigns_stats.html')

        return template.render(limit_experiment=experiment,
                               campaigns=campaigns, tmins=tmins, tmaxs=tmaxs, tmin=str(tmin)[:16], tmax=str(tmax)[:16],
                               do_refresh=1200,
                               next=nextlink, prev=prevlink, tdays=tdays, time_range_string=time_range_string,
                               key='', help_page="ShowCampaignsHelp",
                               experiments=experiments,
                               dbg=kwargs)

    @cherrypy.expose
    @logit.logstartstop
    def campaign_info(self, campaign_id, tmin=None, tmax=None, tdays=None):
        (campaign_info, time_range_string,
         tmins, tmaxs, tdays,
         campaign_definition_info,
         launch_template_info, tags,
         launched_campaigns, dimlist, campaign,
         counts_keys, counts,
         launch_flist,
         kibana_link, dep_svg) = self.campaignsPOMS.campaign_info(cherrypy.request.db,
                                                         cherrypy.request.samweb_lite,
                                                         cherrypy.HTTPError,
                                                         cherrypy.config.get,
                                                         campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_info.html')
        return template.render(Campaign_info=campaign_info, time_range_string=time_range_string, tmins=tmins, tmaxs=tmaxs,
                               Campaign_definition_info=campaign_definition_info, Launch_template_info=launch_template_info,
                               tags=tags, launched_campaigns=launched_campaigns, dimlist=dimlist,
                               Campaign=campaign, counts_keys=counts_keys, counts=counts, launch_flist=launch_flist,
                               do_refresh=0, help_page="CampaignInfoHelp",
                               kibana_link=kibana_link, dep_svg = dep_svg)


    @cherrypy.expose
    @logit.logstartstop
    def campaign_time_bars(self, campaign_id=None, tag=None, tmin=None, tmax=None, tdays=1):
        (
            job_counts, blob, name, tmin, tmax, nextlink, prevlink, tdays, key, extramap
        ) = self.campaignsPOMS.campaign_time_bars(cherrypy.request.db,
                                                  campaign_id=campaign_id,
                                                  tag=tag,
                                                  tmin=tmin, tmax=tmax, tdays=tdays)
        template = self.jinja_env.get_template('campaign_time_bars.html')
        return template.render(job_counts=job_counts, blob=blob, name=name, tmin=tmin, tmax=tmax,
                               do_refresh=1200, next=nextlink, prev=prevlink, tdays=tdays, key=key,
                               extramap=extramap, help_page="CampaignTimeBarsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def register_poms_campaign(self, experiment, campaign_name, version, user=None,
                               campaign_definition=None, dataset="", role="Analysis", params=[]):
        campaign_id = self.campaignsPOMS.register_poms_campaign(cherrypy.request.db,
                                                                experiment,
                                                                campaign_name,
                                                                version, user,
                                                                campaign_definition,
                                                                dataset, role, params)
        return "Campaign=%d" % campaign_id


    @cherrypy.expose
    @logit.logstartstop
    def list_launch_file(self, campaign_id = None, fname = None, launch_template_id = None):
        lines, refresh = self.campaignsPOMS.list_launch_file(campaign_id, fname, launch_template_id)
        output = "".join(lines)
        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command='', output=output, do_refresh=refresh,
                              c=None, campaign_id=campaign_id,
                              help_page="LaunchedJobsHelp")
        return res


    @cherrypy.expose
    @logit.logstartstop
    def schedule_launch(self, campaign_id):
        c, job, launch_flist = self.campaignsPOMS.schedule_launch(cherrypy.request.db, campaign_id)
        template = self.jinja_env.get_template('schedule_launch.html')
        return template.render(c=c, campaign_id=campaign_id, job=job,
                               do_refresh=0, help_page="ScheduleLaunchHelp",
                               launch_flist=launch_flist)


    @cherrypy.expose
    @logit.logstartstop
    def update_launch_schedule(self, campaign_id, dowlist=None, domlist=None,
                               monthly=None, month=None, hourlist=None, submit=None, minlist=None, delete=None):
        self.campaignsPOMS.update_launch_schedule(campaign_id, dowlist, domlist, monthly, month, hourlist, submit, minlist, delete)
        raise cherrypy.HTTPRedirect("schedule_launch?campaign_id=%s" % campaign_id)


    @cherrypy.expose
    @logit.logstartstop
    def mark_campaign_active(self, campaign_id=None, is_active="", cl=None):
        logit.log("cl={}; is_active='{}'".format(cl, is_active))
        campaign_ids = (campaign_id or cl).split(",")
        for cid in campaign_ids:
            campaign = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == cid).first()
            if campaign and cherrypy.session.get('experimenter').is_authorized(campaign.experiment):
                campaign.active = (is_active in ('True', 'Active'))
                cherrypy.request.db.add(campaign)
                cherrypy.request.db.commit()
            else:
                raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        if campaign_id:
            raise cherrypy.HTTPRedirect("campaign_info?campaign_id=%s" % campaign_id)
        elif cl:
            raise cherrypy.HTTPRedirect("show_campaigns")


    @cherrypy.expose
    @logit.logstartstop
    def make_stale_campaigns_inactive(self):
        # TODO: not finished yet!
        # XXXX 'c' below does not exist!
        #if not cherrypy.session.get('experimenter').is_authorized(c.experiment):
        #    raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        res = self.campaignsPOMS.make_stale_campaigns_inactive(cherrypy.request.db, cherrypy.HTTPError)
        return "Marked inactive stale: " + ",".join(res)
#--------------------------------------
###############
### Tables

    @cherrypy.expose
    @logit.logstartstop
    def list_generic(self, classname):
        l = self.tablesPOMS.list_generic(cherrypy.request.db, cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session, classname)
        template = self.jinja_env.get_template('list_generic.html')
        return template.render(classname=classname,
                               list=l, edit_screen="edit_screen_generic",
                               primary_key='experimenter_id',
                               help_page="ListGenericHelp")



    @cherrypy.expose
    @logit.logstartstop
    def edit_screen_generic(self, classname, id=None):
        return self.tablesPOMS.edit_screen_generic(cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session, classname, id)


    @cherrypy.expose
    @logit.logstartstop
    def update_generic(self, classname, *args, **kwargs):
        return self.tablesPOMS.update_generic(cherrypy.request.db, cherrypy.request.headers.get, cherrypy.session, classname, *args, **kwargs)


    def edit_screen_for(self, classname, eclass, update_call, primkey, primval, valmap):    # XXXX Why this function is not expose
        screendata = self.tablesPOMS.edit_screen_for(cherrypy.request.db,
                                                     cherrypy.request.headers.get,
                                                     cherrypy.session, classname, eclass,
                                                     update_call, primkey, primval, valmap)
        template = self.jinja_env.get_template('edit_screen_for.html')
        return template.render(screendata=screendata, action="./" + update_call,
                               classname=classname,
                               help_page="GenericEditHelp")

#######
### JobPOMS

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def active_jobs(self):
        res = self.jobsPOMS.active_jobs(cherrypy.request.db)
        return list(res)


    @cherrypy.expose
    @logit.logstartstop
    def report_declared_files(self, flist):
        self.filesPOMS.report_declared_files(flist, cherrypy.request.db)
        return "Ok."


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def output_pending_jobs(self):
        res = self.jobsPOMS.output_pending_jobs(cherrypy.request.db)
        return res

    @cherrypy.expose
    @logit.logstartstop
    def bulk_update_job(self, data='{}'):
        if not cherrypy.session.get('experimenter').is_root():
            cherrypy.log("update_job: not allowed")
            return "Not Allowed"
        return self.jobsPOMS.bulk_update_job(cherrypy.request.db,
                                             cherrypy.response.status, cherrypy.request.samweb_lite, data)


    @cherrypy.expose
    @logit.logstartstop
    def update_job(self, jobsub_job_id, task_id=None, **kwargs):
        cherrypy.log("update_job( task_id %s, jobsub_job_id %s,  kwargs %s )" % (task_id, jobsub_job_id, repr(kwargs)))
        if not cherrypy.session.get('experimenter').is_root():
            cherrypy.log("update_job: not allowed")
            return "Not Allowed"
        return self.jobsPOMS.update_job(cherrypy.request.db, cherrypy.response.status,
                                        cherrypy.request.samweb_lite, task_id, jobsub_job_id, **kwargs)


    @cherrypy.expose
    @logit.logstartstop
    def test_job_conts(self, task_id=None, campaign_id=None):
        res = self.triagePOMS.job_counts(cherrypy.request.db, task_id, campaign_id)
        return repr(res) + self.filesPOMS.format_job_counts(task_id, campaign_id)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_job_counts(self, task_id=None, campaign_id=None, tmin=None, tmax=None, uuid = None):
        return  self.triagePOMS.job_counts(cherrypy.request.db, task_id, campaign_id, tmin=tmin, tmax=tmax, tdays=None)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_pending_for_campaigns(self, cl, tmin, tmax,uuid=None):
        res = self.filesPOMS.get_pending_dict_for_campaigns(cherrypy.request.db, cherrypy.request.samweb_lite, cl, tmin, tmax)
        return res

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_efficiency_for_campaigns(self, cl, tmin, tmax, uuid=None):
        res = self.jobsPOMS.get_efficiency_map(cherrypy.request.db, cl, tmin, tmax)
        return res

    @cherrypy.expose
    @logit.logstartstop
    def kill_jobs(self, campaign_id=None, task_id=None, job_id=None, confirm=None):
        if confirm is None:
            jjil, t, campaign_id, task_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, campaign_id, task_id, job_id, confirm)
            template = self.jinja_env.get_template('kill_jobs_confirm.html')
            return template.render(jjil=jjil, task=t, campaign_id=campaign_id,
                                   task_id=task_id, job_id=job_id,
                                   help_page="KilledJobsHelp")

        else:
            output, c, campaign_id, task_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, campaign_id, task_id, job_id, confirm)
            template = self.jinja_env.get_template('kill_jobs.html')
            return template.render(output=output,
                                   c=c, campaign_id=campaign_id, task_id=task_id,
                                   job_id=job_id,
                                   help_page="KilledJobsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def jobs_time_histo(self, campaign_id, timetype, tmax=None, tmin=None, tdays=1,binsize=None, submit=None):
        (c, maxv, maxbucket, total, vals, binsize,
            tmaxs, campaign_id,
            tdays, tmin, tmax,
            nextlink, prevlink, tdays) = self.jobsPOMS.jobs_time_histo(cherrypy.request.db, campaign_id, timetype, binsize, tmax, tmin, tdays)
        template = self.jinja_env.get_template('jobs_time_histo.html')
        return template.render(c=c, maxv=maxv, total=total,
                               timetype=timetype, binsize=binsize, maxbucket=maxbucket,
                               maxtime=max(list(vals.keys())),
                               vals=vals, tmaxs=tmaxs,
                               campaign_id=campaign_id,
                               tdays=tdays, tmin=tmin, tmax=tmax,
                               do_refresh=1200, next=nextlink, prev=prevlink,
                               help_page="JobEfficiencyHistoHelp")



    @cherrypy.expose
    @logit.logstartstop
    def jobs_eff_histo(self, campaign_id, tmax=None, tmin=None, tdays=1):
        (c, maxv, total, vals,
            tmaxs, campaign_id,
            tdays, tmin, tmax,
            nextlink, prevlink, tdays) = self.jobsPOMS.jobs_eff_histo(cherrypy.request.db, campaign_id, tmax, tmin, tdays)
        template = self.jinja_env.get_template('jobs_eff_histo.html')
        return template.render(c=c, maxv=maxv, total=total,
                               maxeff=max(list(vals.keys())+[10]),
                               vals=vals, tmaxs=tmaxs,
                               campaign_id=campaign_id,
                               tdays=tdays, tmin=tmin, tmax=tmax,
                               do_refresh=1200, next=nextlink, prev=prevlink,
                               help_page="JobEfficiencyHistoHelp")


    @cherrypy.expose
    @logit.logstartstop
    def set_job_launches(self, hold):
        self.taskPOMS.set_job_launches(cherrypy.request.db, hold)
        raise cherrypy.HTTPRedirect(self.path + "/")

    @cherrypy.expose
    @logit.logstartstop
    def launch_queued_job(self):
        return self.taskPOMS.launch_queued_job(cherrypy.request.db,
                                               cherrypy.request.samweb_lite,
                                               cherrypy.session, cherrypy.request.headers.get,
                                               cherrypy.session, cherrypy.response.status)

    @cherrypy.expose
    @logit.logstartstop
    def launch_jobs(self, campaign_id, dataset_override=None, parent_task_id=None, test_launch_template = None):     # XXXX needs to be analize in detail.
        vals = self.taskPOMS.launch_jobs(cherrypy.request.db,
                                         cherrypy.config.get,
                                         cherrypy.request.headers.get,
                                         cherrypy.session.get,
                                         cherrypy.request.samweb_lite,
                                         cherrypy.response.status, campaign_id, dataset_override = dataset_override, parent_task_id = parent_task_id, test_launch_template = test_launch_template)
        logit.log("Got vals: %s" % repr(vals))
        lcmd, c, campaign_id, outdir, outfile = vals
        if (lcmd == "") :
            return "Launches held, job queued..."
        else:
            if test_launch_template:
                raise cherrypy.HTTPRedirect("%s/list_launch_file?launch_template_id=%s&fname=%s" % (self.path, test_launch_template, os.path.basename(outfile)))
            else:
                raise cherrypy.HTTPRedirect("%s/list_launch_file?campaign_id=%s&fname=%s" % (self.path, campaign_id, os.path.basename(outfile)))

#----------------------
########################
### TaskPOMS

    @cherrypy.expose
    @logit.logstartstop
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor):
        # FIXME: can_create_task() does not exist!
        if not can_create_task():
            return "Not Allowed"
        return (self.taskPOMS.create_task(cherrypy.request.db,
                                          experiment, taskdef, params,
                                          input_dataset, output_dataset, creator, waitingfor))

    @cherrypy.expose
    @logit.logstartstop
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return "\n".join(self.taskPOMS.wrapup_tasks(cherrypy.request.db,
                                                    cherrypy.request.samweb_lite,
                                                    cherrypy.config.get,
                                                    cherrypy.request.headers.get,
                                                    cherrypy.session,
                                                    cherrypy.response.status))

    @cherrypy.expose
    @logit.logstartstop
    def show_task_jobs(self, task_id, tmax=None, tmin=None, tdays=1):   # XXXX Need to be tested HERE
        (blob, job_counts,
            task_id, tmin, tmax,
            extramap, key, task_jobsub_id,
            campaign_id, cname) = self.taskPOMS.show_task_jobs(cherrypy.request.db, task_id, tmax, tmin, tdays)
        template = self.jinja_env.get_template('show_task_jobs.html')
        return template.render(blob=blob, job_counts=job_counts,
                               taskid=task_id, tmin=tmin, tmax=tmax,
                               extramap=extramap, do_refresh=1200, key=key,
                               help_page="ShowTaskJobsHelp",
                               task_jobsub_id=task_jobsub_id,
                               campaign_id=campaign_id, cname=cname)


    @cherrypy.expose
    @logit.logstartstop
    def get_task_id_for(self, campaign, user=None, experiment=None, command_executed="", input_dataset="", parent_task_id=None):
        task_id = self.taskPOMS.get_task_id_for(cherrypy.request.db, campaign, user,
                                                experiment, command_executed, input_dataset, parent_task_id)
        return "Task=%d" % task_id
#------------------------
#########################
### FilesPOMS

    @cherrypy.expose
    @logit.logstartstop
    def list_task_logged_files(self, task_id):
        fl, t, jobsub_job_id = self.filesPOMS.list_task_logged_files(cherrypy.request.db, task_id)
        template = self.jinja_env.get_template('list_task_logged_files.html')
        return template.render(fl=fl, campaign=t.campaign_snap_obj, jobsub_job_id=jobsub_job_id,
                               do_refresh=0,
                               help_page="ListTaskLoggedFilesHelp")


    @cherrypy.expose
    @logit.logstartstop
    def campaign_task_files(self, campaign_id, tmin=None, tmax=None, tdays=1):
        (c, columns, datarows,
            tmins, tmaxs,
            prevlink, nextlink, tdays) = self.filesPOMS.campaign_task_files(cherrypy.request.db, cherrypy.request.samweb_lite, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_task_files.html')
        return template.render(name=c.name if c else "",
                               columns=columns, datarows=datarows,
                               tmin=tmins, tmax=tmaxs,
                               prev=prevlink, next=nextlink, tdays=tdays,
                               campaign_id=campaign_id, help_page="CampaignTaskFilesHelp")


    @cherrypy.expose
    @logit.logstartstop
    def job_file_list(self, job_id, force_reload=False):    # XXXX Ask Marc to check this in the module
        return self.filesPOMS.job_file_list(cherrypy.request.db, cherrypy.request.jobsub_fetcher, job_id, force_reload)


    @cherrypy.expose
    @logit.logstartstop
    def job_file_contents(self, job_id, task_id, file, tmin=None, tmax=None, tdays=None):
        job_file_contents, tmin = self.filesPOMS.job_file_contents(cherrypy.request.db,
                                                                   cherrypy.request.jobsub_fetcher,
                                                                   job_id, task_id, file, tmin, tmax, tdays)
        template = self.jinja_env.get_template('job_file_contents.html')
        return template.render(file=file, job_file_contents=job_file_contents,
                               task_id=task_id, job_id=job_id, tmin=tmin,
                               help_page="JobFileContentsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def inflight_files(self, campaign_id=None, task_id=None):
        outlist, statusmap, c = self.filesPOMS.inflight_files(cherrypy.request.db,
                                                              cherrypy.response.status,
                                                              cherrypy.request.app.config['POMS'].get,
                                                              campaign_id, task_id)
        template = self.jinja_env.get_template('inflight_files.html')
        return template.render(flist=outlist,
                               statusmap=statusmap, c=c,
                               jjid=self.taskPOMS.task_min_job(cherrypy.request.db, task_id),
                               campaign_id=campaign_id, task_id=task_id,
                               help_page="PendingFilesJobsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def show_dimension_files(self, experiment, dims):
        flist = self.filesPOMS.show_dimension_files(cherrypy.request.samweb_lite, experiment, dims, dbhandle=cherrypy.request.db)
        template = self.jinja_env.get_template('show_dimension_files.html')
        return template.render(flist=flist, dims=dims, statusmap=[], help_page="ShowDimensionFilesHelp")


    @cherrypy.expose
    @logit.logstartstop
    def actual_pending_files(self, count_or_list = None, campaign_id=None, tmin=None, tmax=None, tdays=1):
        exps, dims = self.filesPOMS.actual_pending_file_dims(cherrypy.request.db,
                                                             cherrypy.request.samweb_lite,
                                                             campaign_id=campaign_id,
                                                             tmin=tmin, tmax=tmax, tdays=tdays)
        return self.show_dimension_files(exps[0], dims[0])

    @cherrypy.expose
    @logit.logstartstop
    def campaign_sheet(self, campaign_id, tmin=None, tmax=None, tdays=7):
        (name, columns, outrows, dimlist,
            experiment, tmaxs,
            prevlink, nextlink,
            tdays, tmin, tmax) = self.filesPOMS.campaign_sheet(cherrypy.request.db,
                                                               cherrypy.request.samweb_lite,
                                                               campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_sheet.html')
        return template.render(name=name,
                               columns=columns,
                               datarows=outrows,
                               dimlist=dimlist,
                               tmaxs=tmaxs,
                               prev=prevlink,
                               next=nextlink,
                               tdays=tdays,
                               tmin=tmin,
                               tmax=tmax,
                               campaign_id=campaign_id,
                               experiment=experiment,
                               help_page="CampaignSheetHelp")



    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_project_summary_for_task(self, task_id):
        return self.project_summary_for_task(task_id)


    def project_summary_for_task(self, task_id):
        t = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).first()
        return cherrypy.request.samweb_lite.fetch_info(t.campaign_snap_obj.experiment, t.project, dbhandle=cherrypy.request.db)


    def project_summary_for_tasks(self, task_list):
        return cherrypy.request.samweb_lite.fetch_info_list(task_list, dbhandle=cherrypy.request.db)
        #~ return [ {"tot_consumed": 0, "tot_failed": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug
###Im here
#----------------------------
##########
### TriagePOMS

    @cherrypy.expose
    @logit.logstartstop
    def triage_job(self, job_id, tmin=None, tmax=None, tdays=None, force_reload=False):
        (job_file_list, job_info, job_history,
            downtimes, output_file_names_list,
            es_response, efficiency,
            tmin, task_jobsub_job_id) = self.triagePOMS.triage_job(cherrypy.request.db,
                                                                   cherrypy.request.jobsub_fetcher,
                                                                   cherrypy.config,
                                                                   job_id, tmin, tmax, tdays, force_reload)
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
                               help_page="TriageJobHelp",
                               task_jobsub_job_id=task_jobsub_job_id)



    @cherrypy.expose
    @logit.logstartstop
    def job_table(self, offset=0, **kwargs):
        ###The pass of the arguments is ugly we will fix that later.
        (jlist, jobcolumns, taskcolumns,
            campcolumns, tmins, tmaxs,
            prevlink, nextlink, tdays,
            extra, hidecolumns, filtered_fields,
            time_range_string) = self.triagePOMS.job_table(cherrypy.request.db, cherrypy.session, **kwargs)

        template = self.jinja_env.get_template('job_table.html')

        return template.render(joblist=jlist,
                               jobcolumns=jobcolumns,
                               taskcolumns=taskcolumns,
                               campcolumns=campcolumns,
                               tmin=tmins,
                               tmax=tmaxs,
                               prev=prevlink,
                               next=nextlink,
                               tdays=tdays,
                               extra=extra,
                               hidecolumns=hidecolumns,
                               filtered_fields=filtered_fields,
                               time_range_string=time_range_string,
                               offset=int(offset),
                               do_refresh=0,
                               help_page="JobTableHelp")


    @cherrypy.expose
    @logit.logstartstop
    def jobs_by_exitcode(self, tmin=None, tmax=None, tdays=1):
        raise cherrypy.HTTPRedirect("%s/failed_jobs_by_whatever?f=user_exe_exit_code&tdays=%s" % (self.path, tdays))


    @cherrypy.expose
    @logit.logstartstop
    def failed_jobs_by_whatever(self, tmin=None, tmax=None, tdays=1, f=[], go=None):
        (jl, possible_columns, columns,
            tmins, tmaxs, tdays,
            prevlink, nextlink,
            time_range_string, tdays) = self.triagePOMS.failed_jobs_by_whatever(cherrypy.request.db, cherrypy.session, tmin, tmax, tdays, f, go)
        template = self.jinja_env.get_template('failed_jobs_by_whatever.html')
        return template.render(joblist=jl,
                               possible_columns=possible_columns,
                               columns=columns,
                               do_refresh=0,
                               tmin=tmins,
                               tmax=tmaxs,
                               tdays=tdays,
                               prev=prevlink,
                               next=nextlink,
                               time_range_string=time_range_string,
                               help_page="JobsByExitcodeHelp")

##############
### TagsPOMS

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def link_tags(self, campaign_id, tag_name, experiment):
        return self.tagsPOMS.link_tags(cherrypy.request.db, cherrypy.session, campaign_id, tag_name, experiment)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def delete_campaigns_tags(self, campaign_id, tag_id, experiment):
        return self.tagsPOMS.delete_campaigns_tags(cherrypy.request.db, cherrypy.session.get, campaign_id, tag_id, experiment)


    @cherrypy.expose
    @logit.logstartstop
    def search_tags(self, q):
        results, q_list = self.tagsPOMS.search_tags(cherrypy.request.db, q)
        template = self.jinja_env.get_template('search_tags.html')
        return template.render(results=results, q_list=q_list, do_refresh=0, help_page="SearchTagsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def auto_complete_tags_search(self, experiment, q):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return self.tagsPOMS.auto_complete_tags_search(cherrypy.request.db, experiment, q)
#-----------------------
# debugging
