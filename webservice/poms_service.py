#
# pylint: disable=line-too-long,invalid-name,missing-docstring
#
import os
import glob
import pprint
import socket

import cherrypy
from jinja2 import Environment, PackageLoader

from . import (
               CampaignsPOMS,
               DBadminPOMS,
               FilesPOMS,
               JobsPOMS,
               TablesPOMS,
               TagsPOMS,
               TaskPOMS,
               UtilsPOMS,
               logit,
               version)
from .elasticsearch import Elasticsearch
from .poms_model import Campaign, CampaignStage, Submission, Experiment


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
                  'error_page.404': "%s/%s" % (os.path.abspath(os.getcwd()), '/templates/page_not_found.html'),
                  'error_page.401': "%s/%s" % (os.path.abspath(os.getcwd()), '/webservice/templates/unauthorized_user.html')
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
        self.dbadminPOMS = DBadminPOMS.DBadminPOMS()
        self.campaignsPOMS = CampaignsPOMS.CampaignsPOMS(self)
        self.jobsPOMS = JobsPOMS.JobsPOMS(self)
        self.taskPOMS = TaskPOMS.TaskPOMS(self)
        self.utilsPOMS = UtilsPOMS.UtilsPOMS(self)
        self.tagsPOMS = TagsPOMS.TagsPOMS(self)
        self.filesPOMS = FilesPOMS.Files_status(self)
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
        return template.render(services='',
                               version= self.version,
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
        self.utilsPOMS.quick_search(cherrypy.HTTPRedirect, search_term)

    @cherrypy.expose
    @logit.logstartstop
    def update_session_experiment(self, *args, **kwargs):
        self.utilsPOMS.update_session_experiment(cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

    @cherrypy.expose
    @logit.logstartstop
    def update_session_role(self, *args, **kwargs):
        return self.utilsPOMS.update_session_role(cherrypy.request.db, cherrypy.session.get, *args, **kwargs)


    #####
    ### DBadminPOMS
    @cherrypy.expose
    @logit.logstartstop
    def raw_tables(self):
        if not cherrypy.session.get('experimenter').is_root():
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('raw_tables.html')
        return template.render(tlist=list(self.tablesPOMS.admin_map.keys()), help_page="RawTablesHelp")


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def experiment_list(self):
        return list(map((lambda x: x[0]),cherrypy.request.db.query(Experiment.experiment).all()))

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

    # -----------------------------------------
    #################################
    ### CampaignsPOMS

    @cherrypy.expose
    @logit.logstartstop
    def launch_template_edit(self, *args, **kwargs):
        #v3_2_0 backward compatabilty
        self.login_setup_edit(args, kwargs)

    @cherrypy.expose
    @logit.logstartstop
    def login_setup_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.login_setup_edit(cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

        if kwargs.get('test_template'):
            raise cherrypy.HTTPRedirect(
                "%s/launch_jobs?campaign_stage_id=None&test_login_setup=%s" % (self.path, data['login_setup_id']))

        template = self.jinja_env.get_template('login_setup_edit.html')
        return template.render(data=data, help_page="LaunchTemplateEditHelp")


    @cherrypy.expose
    @logit.logstartstop
    def campaign_deps_ini(self, tag=None, camp_id=None, login_setup=None, campaign_definition=None, launch_template=None):
        experiment = cherrypy.session.get('experimenter').session_experiment
        res = self.campaignsPOMS.campaign_deps_ini(cherrypy.request.db, cherrypy.config.get, experiment,
                                                   name=tag, camp_id=camp_id,
                                                   login_setup=login_setup or launch_template,
                                                   campaign_definition=campaign_definition)
        cherrypy.response.headers['Content-Type'] = 'text/ini'
        return res


    @cherrypy.expose
    @logit.logstartstop
    def campaign_deps(self, campaign_name=None, campaign_stage_id=None, tag=None, camp_id=None):

        template = self.jinja_env.get_template('campaign_deps.html')
        svgdata = self.campaignsPOMS.campaign_deps_svg(cherrypy.request.db, cherrypy.config.get,
                                                       campaign_name=campaign_name or tag,
                                                       campaign_stage_id=campaign_stage_id or camp_id)
        return template.render(tag=tag, svgdata=svgdata, help_page="CampaignDepsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def campaign_definition_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_definition_edit(cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

        if kwargs.get('test_template'):
            test_campaign = self.campaignsPOMS.make_test_campaign_for(cherrypy.request.db, cherrypy.session,
                                                                      kwargs.get("ae_campaign_definition_id"),
                                                                      kwargs.get("ae_definition_name"))
            raise cherrypy.HTTPRedirect(
                "%s/campaign_edit?jump_to_campaign=%d&extra_edit_flag=launch_test_job" % (self.path, test_campaign))

        template = self.jinja_env.get_template('campaign_definition_edit.html')
        return template.render(data=data, help_page="CampaignDefinitionEditHelp")


    @cherrypy.expose
    @logit.logstartstop
    def make_test_campaign_for(self, campaign_def_id, campaign_def_name):
        cid = self.campaignsPOMS.make_test_campaign_for(cherrypy.request.db, cherrypy.session, campaign_def_id,
                                                        campaign_def_name)
        raise cherrypy.HTTPRedirect(
            "%s/campaign_edit?campaign_stage_id=%d&extra_edit_flag=launch_test_job" % (self.path, cid))


    @cherrypy.expose
    @logit.logstartstop
    def campaign_edit(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        :return:
        """
        # Note we have to use cherrypy.session here instead of cherrypy.session.get method
        # because later need to access cherrypy.session[''] in campaignsPOMS.campaign_edit.
        data = self.campaignsPOMS.campaign_edit(cherrypy.request.db, cherrypy.session, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_edit.html')

        if kwargs.get('pcl_call', '0') == '1':
            if data['message']:
                raise cherrypy.HTTPError(400, data['message'])

        if kwargs.get('launch_test_job', None) and kwargs.get('ae_campaign_id', None):
            raise cherrypy.HTTPRedirect("%s/launch_jobs?campaign_stage_id=%s" % (self.path, kwargs.get('ae_campaign_id')))

        return template.render(data=data, help_page="CampaignEditHelp",
                               extra_edit_flag=kwargs.get("extra_edit_flag", None),
                               jump_to_campaign=kwargs.get("jump_to_campaign", None)
                               )


    @cherrypy.expose
    @logit.logstartstop
    def gui_wf_edit(self, *args, **kwargs):
        template = self.jinja_env.get_template('gui_wf_edit.html')
        return template.render(help_page="GUI_Workflow_Editor_User_Guide", campaign=kwargs.get('campaign'))


    @cherrypy.expose
    @logit.logstartstop
    def sample_workflows(self, *args, **kwargs):
        sl = [x.replace(os.environ['POMS_DIR'] + '/webservice/static/', '') for x in glob.glob(os.environ['POMS_DIR'] + '/webservice/static/samples/*')]
        logit.log("from %s think we got sl of %s" % (os.environ['POMS_DIR'], ",".join(sl)))
        template = self.jinja_env.get_template('sample_workflows.html')
        return template.render(help_page="Sample Workflows", sl=sl)


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
    def show_campaigns(self, *args, **kwargs):
        experimenter = cherrypy.session.get('experimenter')
        tl, last_activity, msg = self.tagsPOMS.show_campaigns(cherrypy.request.db, experimenter, *args, **kwargs)
        template = self.jinja_env.get_template('show_campaigns.html')
        return template.render(tl=tl, last_activity=last_activity, msg=msg, help_page="ShowCampaignTagsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def show_campaign_stages(self, tmin=None, tmax=None, tdays=7, active=True, tag=None, holder=None, role_held_with=None,
                       se_role=None, cl=None, **kwargs):
        (
            campaign_stages, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string, data
        ) = self.campaignsPOMS.show_campaign_stages(cherrypy.request.db,
                                              cherrypy.request.samweb_lite,
                                              tmin=tmin, tmax=tmax, tdays=tdays, active=active, tag=tag,
                                              holder=holder, role_held_with=role_held_with,
                                              campaign_ids=cl, sesshandler=cherrypy.session.get)

        current_experimenter = cherrypy.session.get('experimenter')
        # ~ logit.log("current_experimenter.extra before: "+str(current_experimenter.extra))     # DEBUG
        if 'exp_selected' in kwargs:
            current_experimenter.extra = {'selected': kwargs['exp_selected']}
            cherrypy.session.acquire_lock()
            cherrypy.session['experimenter'] = current_experimenter
            cherrypy.session.save()
            cherrypy.session.release_lock()
            # ~ logit.log("current_experimenter.extra update... ")                               # DEBUG
        # ~ logit.log("current_experimenter.extra after: "+str(current_experimenter.extra))      # DEBUG

        if cl is None:
            template = self.jinja_env.get_template('show_campaign_stages.html')
        else:
            template = self.jinja_env.get_template('show_campaign_stages_stats.html')

        return template.render(limit_experiment=current_experimenter.session_experiment,
                               campaign_stages=campaign_stages, tmins=tmins, tmaxs=tmaxs, tmin=str(tmin)[:16], tmax=str(tmax)[:16],
                               do_refresh=1200, data=data,
                               next=nextlink, prev=prevlink, tdays=tdays, time_range_string=time_range_string,
                               key='', help_page="ShowCampaignsHelp", dbg=kwargs)


    @cherrypy.expose
    @logit.logstartstop
    def reset_campaign_split(self, campaign_stage_id):
        campaign = cherrypy.request.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        if campaign and cherrypy.session.get('experimenter').is_authorized(campaign):
            res = self.campaignsPOMS.reset_campaign_split(
                cherrypy.request.db,
                cherrypy.request.samweb_lite,
                campaign_stage_id)
            raise cherrypy.HTTPRedirect("campaign_info?campaign_stage_id=%s" % campaign_stage_id)
        else:
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')


    @cherrypy.expose
    @logit.logstartstop
    def campaign_info(self, campaign_stage_id, tmin=None, tmax=None, tdays=None):
        (campaign_info, time_range_string,
         tmins, tmaxs, tdays,
         campaign_definition_info,
         login_setup_info, campaigns,
         launched_campaigns, dimlist, campaign,
         counts_keys, counts,
         launch_flist,
         kibana_link, dep_svg, last_activity) = self.campaignsPOMS.campaign_info(cherrypy.request.db,
                                                                                 cherrypy.request.samweb_lite,
                                                                                 cherrypy.HTTPError,
                                                                                 cherrypy.config.get,
                                                                                 campaign_stage_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_info.html')
        return template.render(
            Campaign_info=campaign_info,
            time_range_string=time_range_string,
            tmins=tmins,
            tmaxs=tmaxs,
            Campaign_definition_info=campaign_definition_info,
            login_setup_info=login_setup_info,
            campaigns=campaigns,
            launched_campaigns=launched_campaigns,
            dimlist=dimlist,
            CampaignStage=campaign,
            counts_keys=counts_keys,
            counts=counts,
            launch_flist=launch_flist,
            do_refresh=0,
            help_page="CampaignInfoHelp",
            kibana_link=kibana_link,
            dep_svg=dep_svg, last_activity=last_activity)


    @cherrypy.expose
    @logit.logstartstop
    def campaign_time_bars(self, campaign_stage_id=None, campaign=None, tag=None, tmin=None, tmax=None, tdays=1):
        if tag != None and campaign == None:
             campaign = tag

        (
            job_counts, blob, name, tmin, tmax, nextlink, prevlink, tdays, key, extramap
        ) = self.campaignsPOMS.campaign_time_bars(cherrypy.request.db,
                                                  campaign_stage_id=campaign_stage_id,
                                                  campaign=campaign,
                                                  tmin=tmin, tmax=tmax, tdays=tdays)
        template = self.jinja_env.get_template('campaign_time_bars.html')
        return template.render(job_counts=job_counts, blob=blob, name=name, tmin=tmin, tmax=tmax,
                               do_refresh=1200, next=nextlink, prev=prevlink, tdays=tdays, key=key,
                               extramap=extramap, help_page="CampaignTimeBarsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def register_poms_campaign(self, experiment, campaign_name, version, user=None,
                               campaign_definition=None, dataset="", role="Analysis",
                               params=[]):
        loguser = cherrypy.session.get('experimenter')

        if loguser.username != 'poms':
            user = loguser.username

        campaign_stage_id = self.campaignsPOMS.register_poms_campaign(cherrypy.request.db,
                                                                      experiment,
                                                                      campaign_name,
                                                                      version, user,
                                                                      campaign_definition,
                                                                      dataset, role, loguser.session_role,
                                                                      cherrypy.session.get, params)
        return "CampaignStage=%d" % campaign_stage_id


    @cherrypy.expose
    @logit.logstartstop
    @logit.logstartstop
    def list_launch_file(self, campaign_stage_id=None, fname=None, login_setup_id=None, launch_template_id=None):
        if login_setup_id is None and launch_template_id is not None:
            login_setup_id = launch_template_id
        lines, refresh = self.campaignsPOMS.list_launch_file(campaign_stage_id, fname, login_setup_id)
        output = "".join(lines)
        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command='', output=output, do_refresh=refresh,
                              cs=None, campaign_stage_id=campaign_stage_id,
                              help_page="LaunchedJobsHelp")
        return res


    @cherrypy.expose
    @logit.logstartstop
    def schedule_launch(self, campaign_stage_id):
        cs, job, launch_flist = self.campaignsPOMS.schedule_launch(cherrypy.request.db, campaign_stage_id)
        template = self.jinja_env.get_template('schedule_launch.html')
        return template.render(cs=cs, campaign_stage_id=campaign_stage_id, job=job,
                               do_refresh=0, help_page="ScheduleLaunchHelp",
                               launch_flist=launch_flist)


    @cherrypy.expose
    @logit.logstartstop
    def update_launch_schedule(self, campaign_stage_id, dowlist=None, domlist=None,
                               monthly=None, month=None, hourlist=None, submit=None, minlist=None, delete=None):
        self.campaignsPOMS.update_launch_schedule(campaign_stage_id, dowlist, domlist, monthly, month, hourlist, submit,
                                                  minlist, delete, user=cherrypy.session.get('experimenter').experimenter_id)
        raise cherrypy.HTTPRedirect("schedule_launch?campaign_stage_id=%s" % campaign_stage_id)


    @cherrypy.expose
    @logit.logstartstop
    def mark_campaign_active(self, campaign_stage_id=None, is_active="", cl=None):

        logit.log("cl={}; is_active='{}'".format(cl, is_active))
        campaign_ids = (campaign_stage_id or cl).split(",")
        for cid in campaign_ids:
            auth = False
            campaign = cherrypy.request.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == cid).first()
            if campaign:
                user = cherrypy.session.get('experimenter')
                if user.is_root():
                    auth = True
                elif user.session_experiment == campaign.experiment:
                    if user.is_coordinator():
                        auth = True
                    elif user.is_production() and campaign.creator_role == 'production':
                        auth = True
                    elif user.session_role == campaign.creator_role and user.experimenter_id == campaign.creator:
                        auth = True
                    else:
                        raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
                else:
                    raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
                if auth:
                    campaign.active = (is_active in ('True', 'Active', 'true', '1'))
                    cherrypy.request.db.add(campaign)
                    cherrypy.request.db.commit()
                else:
                    raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        if campaign_stage_id:
            raise cherrypy.HTTPRedirect("campaign_info?campaign_stage_id=%s" % campaign_stage_id)
        elif cl:
            raise cherrypy.HTTPRedirect("show_campaign_stages")


    @cherrypy.expose
    @logit.logstartstop
    def mark_campaign_hold(self, ids2HR=None, is_hold=''):
        """
            Who can hold/release a campaign:
            The creator can hold/release her/his own campaign_stages.
            The root can hold/release any campaign_stages.
            The coordinator can hold/release any campaign_stages that in the same experiment as the coordinator.
            Anyone with a production role can hold/release a campaign created with a production role.

            :param  ids2HR: A list of campaign ids to be hold/released.
            :param is_hold: 'Hold' or 'Release'
            :return:
        """
        campaign_ids = ids2HR.split(",")
        sessionExperimenter = cherrypy.session.get('experimenter')
        for cid in campaign_ids:
            campaign = cherrypy.request.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == cid).first()
            if not campaign:
                raise cherrypy.HTTPError(404, 'The campaign campaign_stage_id={} cannot be found.'.format(cid))
            mayIChangeIt = False
            if sessionExperimenter.is_root():
                mayIChangeIt = True
            elif sessionExperimenter.is_coordinator() and sessionExperimenter.session_experiment == campaign.experiment:
                mayIChangeIt = True
            elif sessionExperimenter.is_production() and sessionExperimenter.session_experiment == campaign.experiment \
                and campaign.creator_role == "production":
                mayIChangeIt = True
            elif campaign.creator == sessionExperimenter.experimenter_id and campaign.experiment == sessionExperimenter.session_experiment \
                and campaign.creator_role == sessionExperimenter.session_role:
                mayIChangeIt = True
            else:
                raise cherrypy.HTTPError(401, 'You are not authorized to hold or release this campaign_stages. ')

            if mayIChangeIt:
                if is_hold == "Hold":
                    campaign.hold_experimenter_id = sessionExperimenter.experimenter_id
                    campaign.role_held_with = sessionExperimenter.session_role
                elif is_hold == "Release":
                    campaign.hold_experimenter_id = None
                    campaign.role_held_with = None
                else:
                    raise cherrypy.HTTPError(400, 'The action is not supported. You can only hold or release.')
                cherrypy.request.db.add(campaign)
                cherrypy.request.db.commit()
            else:
                raise cherrypy.HTTPError(401, 'You are not authorized to hold or release this campaign_stages. ')

        if ids2HR:
            raise cherrypy.HTTPRedirect("show_campaign_stages")


    @cherrypy.expose
    @logit.logstartstop
    def make_stale_campaigns_inactive(self):
        if not cherrypy.session.get('experimenter').is_root():
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        res = self.campaignsPOMS.make_stale_campaigns_inactive(cherrypy.request.db, cherrypy.HTTPError)
        return "Marked inactive stale: " + ",".join(res)


    @cherrypy.expose
    @logit.logstartstop
    def list_generic(self, classname):
        l = self.tablesPOMS.list_generic(cherrypy.request.db, cherrypy.HTTPError, cherrypy.request.headers.get,
                                         cherrypy.session, classname)
        template = self.jinja_env.get_template('list_generic.html')
        return template.render(classname=classname,
                               list=l, edit_screen="edit_screen_generic",
                               primary_key='experimenter_id',
                               help_page="ListGenericHelp")

    @cherrypy.expose
    @logit.logstartstop
    def edit_screen_generic(self, classname, id=None):
        return self.tablesPOMS.edit_screen_generic(cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session,
                                                   classname, id)

    @cherrypy.expose
    @logit.logstartstop
    def update_generic(self, classname, *args, **kwargs):
        return self.tablesPOMS.update_generic(cherrypy.request.db, cherrypy.request.headers.get, cherrypy.session,
                                              classname, *args, **kwargs)

    @cherrypy.expose
    @logit.logstartstop
    def edit_screen_for(self, classname, eclass, update_call, primkey, primval, valmap):
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
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def running_submissions(self,campaign_id_list):
        cl = list(map(int, campaign_id_list.split(',')))
        return self.taskPOMS.running_submissions(cherrypy.request.db, cl)

    @cherrypy.expose
    @logit.logstartstop
    def update_submission(self, submission_id, jobsub_job_id, pct_complete = None, status = None, project = None):
        res = self.taskPOMS.update_submission(cherrypy.request.db, submission_id, jobsub_job_id, status  = status, project = project, pct_complete = pct_complete)
        return res


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_pending_for_campaigns(self, cl, tmin, tmax, uuid=None):
        res = self.filesPOMS.get_pending_dict_for_campaigns(cherrypy.request.db, cherrypy.request.samweb_lite, cl, tmin, tmax)
        return res


    @cherrypy.expose
    @logit.logstartstop
    def kill_jobs(self, campaign_stage_id=None, submission_id=None, task_id=None, job_id=None, confirm=None, act='kill'):
        if task_id != None and submission_id == None:
            submission_id = task_id
        if confirm is None:
            jjil, s, campaign_stage_id, submission_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, campaign_stage_id, submission_id,
                                                                                        job_id, confirm, act)
            template = self.jinja_env.get_template('kill_jobs_confirm.html')
            return template.render(jjil=jjil, task=s, campaign_stage_id=campaign_stage_id,
                                   submission_id=submission_id, job_id=job_id, act=act,
                                   help_page="KilledJobsHelp")

        else:
            output, cs, campaign_stage_id, submission_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, campaign_stage_id, submission_id,
                                                                              job_id, confirm, act)
            template = self.jinja_env.get_template('kill_jobs.html')
            return template.render(output=output,
                                   cs=cs, campaign_stage_id=campaign_stage_id, submission_id=submission_id,
                                   job_id=job_id, act=act,
                                   help_page="KilledJobsHelp")


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
    def launch_jobs(self, campaign_stage_id, dataset_override=None, parent_submission_id=None, parent_task_id=None, test_login_setup=None,
                    experiment=None, launcher=None, test_launch=False, test_launch_template=None):
        if test_login_setup is None and test_launch_template is not None:
            test_login_setup = test_launch_template
        if parent_task_id is not None and parent_submission_id is None:
            parent_submission_id = parent_task_id
        if cherrypy.session.get('experimenter').username and ('poms' != cherrypy.session.get('experimenter').username or launcher == ''):
            launch_user = cherrypy.session.get('experimenter').experimenter_id
        else:
            launch_user = launcher

        vals = self.taskPOMS.launch_jobs(cherrypy.request.db,
                                         cherrypy.config.get,
                                         cherrypy.request.headers.get,
                                         cherrypy.session.get,
                                         cherrypy.request.samweb_lite,
                                         cherrypy.response.status, campaign_stage_id,
                                         launch_user,
                                         dataset_override=dataset_override,
                                         parent_submission_id=parent_submission_id, test_login_setup=test_login_setup,
                                         experiment=experiment,
                                         test_launch=test_launch)
        logit.log("Got vals: %s" % repr(vals))
        lcmd, cs, campaign_stage_id, outdir, outfile = vals
        if lcmd == "":
            return "Launches held, job queued..."
        else:
            if test_login_setup:
                raise cherrypy.HTTPRedirect("%s/list_launch_file?login_setup_id=%s&fname=%s" % (
                    self.path, test_login_setup, os.path.basename(outfile)))
            else:
                raise cherrypy.HTTPRedirect(
                    "%s/list_launch_file?campaign_stage_id=%s&fname=%s" % (self.path, campaign_stage_id, os.path.basename(outfile)))


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def jobtype_list(self, *args, **kwargs):
        data = self.jobsPOMS.jobtype_list(cherrypy.request.db, cherrypy.session.get)
        return data



    # ----------------------
    ########################
    ### TaskPOMS

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
    def get_task_id_for(self, campaign, user=None, experiment=None, command_executed="", input_dataset="",
                        parent_task_id=None, task_id=None, parent_submission_id = None, submission_id = None):
        if task_id is not None and submission_id is None:
            submission_id = task_id
        if parent_task_id is not None and parent_submission_id is None:
            parent_submission_id = parent_task_id
        submission_id = self.taskPOMS.get_task_id_for(cherrypy.request.db, campaign, user,
                                                experiment, command_executed, input_dataset, parent_submission_id, submission_id)
        return "Task=%d" % submission_id

    @cherrypy.expose
    @logit.logstartstop
    def list_task_logged_files(self, submission_id = None, task_id = None):
        if task_id is not None and submission_id is None:
            submission_id = task_id
        fl, s, jobsub_job_id = self.filesPOMS.list_task_logged_files(cherrypy.request.db, submission_id)
        template = self.jinja_env.get_template('list_task_logged_files.html')
        return template.render(fl=fl, campaign=s.campaign_stage_snapshot_obj, jobsub_job_id=jobsub_job_id,
                               do_refresh=0,
                               help_page="ListTaskLoggedFilesHelp")


    @cherrypy.expose
    @logit.logstartstop
    def campaign_task_files(self, campaign_stage_id, tmin=None, tmax=None, tdays=1):
        (cs, columns, datarows,
         tmins, tmaxs,
         prevlink, nextlink, tdays) = self.filesPOMS.campaign_task_files(cherrypy.request.db,
                                                                         cherrypy.request.samweb_lite, campaign_stage_id,
                                                                         tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_task_files.html')
        return template.render(name=cs.name if cs else "",
                               columns=columns, datarows=datarows,
                               tmin=tmins, tmax=tmaxs,
                               prev=prevlink, next=nextlink, tdays=tdays,
                               campaign_stage_id=campaign_stage_id, help_page="CampaignTaskFilesHelp")

    @cherrypy.expose
    @logit.logstartstop
    def inflight_files(self, campaign_stage_id=None, submission_id=None, task_id = None):
        if task_id != None and submission_id == None:
            submission_id = task_id
        outlist, statusmap, cs = self.filesPOMS.inflight_files(cherrypy.request.db,
                                                               cherrypy.response.status,
                                                               cherrypy.request.app.config['POMS'].get,
                                                               campaign_stage_id, submission_id)
        template = self.jinja_env.get_template('inflight_files.html')
        return template.render(flist=outlist,
                               statusmap=statusmap, cs=cs,
                               jjid='fix_me',
                               campaign_stage_id=campaign_stage_id, submission_id=submission_id,
                               help_page="PendingFilesJobsHelp")


    @cherrypy.expose
    @logit.logstartstop
    def show_dimension_files(self, experiment, dims):
        flist = self.filesPOMS.show_dimension_files(cherrypy.request.samweb_lite, experiment, dims,
                                                    dbhandle=cherrypy.request.db)
        template = self.jinja_env.get_template('show_dimension_files.html')
        return template.render(flist=flist, dims=dims, statusmap=[], help_page="ShowDimensionFilesHelp")


    @cherrypy.expose
    @logit.logstartstop
    def actual_pending_files(self, count_or_list=None, campaign_stage_id=None, tmin=None, tmax=None, tdays=1):
        exps, dims = self.filesPOMS.actual_pending_file_dims(cherrypy.request.db,
                                                             cherrypy.request.samweb_lite,
                                                             campaign_stage_id=campaign_stage_id,
                                                             tmin=tmin, tmax=tmax, tdays=tdays)
        return self.show_dimension_files(exps[0], dims[0])


    @cherrypy.expose
    @logit.logstartstop
    def campaign_sheet(self, campaign_stage_id, tmin=None, tmax=None, tdays=7):
        (name, columns, outrows, dimlist,
         experiment, tmaxs,
         prevlink, nextlink,
         tdays, tmin, tmax) = self.filesPOMS.campaign_sheet(cherrypy.request.db,
                                                            cherrypy.request.samweb_lite,
                                                            campaign_stage_id, tmin, tmax, tdays)
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
                               campaign_stage_id=campaign_stage_id,
                               experiment=experiment,
                               help_page="CampaignSheetHelp")

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_project_summary_for_task(self, submission_id = None, task_id = None):
        if task_id is not None and submission_id is None:
            submission_id = task_id
        return self.project_summary_for_task(submission_id)


    def project_summary_for_task(self, submission_id = None, task_id = None):
        if task_id is not None and submission_id is None:
            submission_id = task_id
        s = cherrypy.request.db.query(Submission).filter(Submission.submission_id == submission_id).first()
        return cherrypy.request.samweb_lite.fetch_info(s.campaign_stage_snapshot_obj.experiment, s.project,
                                                       dbhandle=cherrypy.request.db)

    def project_summary_for_tasks(self, task_list):
        return cherrypy.request.samweb_lite.fetch_info_list(task_list, dbhandle=cherrypy.request.db)
    ##############
    ### TagsPOMS

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def link_tags(self, campaign_stage_id=None, tag_name=None, campaign_name=None, experiment=None, campaign_id=None):
        return self.tagsPOMS.link_tags(cherrypy.request.db, cherrypy.session.get,
                                       campaign_stage_id=campaign_stage_id or campaign_id,
                                       campaign_name=campaign_name or tag_name, experiment=experiment)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def delete_campaigns_tags(self, campaign_stage_id, campaign_id, experiment):
        return self.tagsPOMS.delete_campaigns_tags(cherrypy.request.db, cherrypy.session.get, campaign_stage_id, campaign_id,
                                                   experiment)

    @cherrypy.expose
    @logit.logstartstop
    def search_tags(self, search_term):
        results = self.tagsPOMS.search_tags(cherrypy.request.db, search_term)
        template = self.jinja_env.get_template('search_tags.html')
        return template.render(results=results, search_term=search_term, do_refresh=0, help_page="SearchTagsHelp")


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def search_all_tags(self, cl):
        return self.tagsPOMS.search_all_tags(cherrypy.request.db, cl)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def auto_complete_tags_search(self, experiment, q):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return self.tagsPOMS.auto_complete_tags_search(cherrypy.request.db, experiment, q)

# -----------------------
# debugging
