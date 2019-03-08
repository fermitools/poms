
# h2. Module webservice.poms_service
#
# This module attaches all the webservice methods to
# the cherrypy instance, via the mount call in source:webservice/service.py
# the methods call out to one of the POMS logic modules, and
# generally either use jinja2 templates to render the results, or
# use the cherrypy @cherrypy.tools.json_out() decorator to
# yeild the result in JSON.
#
# h2. Administrivia
#
# h3. pylint
#
# we leave a note here for pylint as we're still getting the
# code pylint clean...
#
# pylint: disable=invalid-name,missing-docstring
#
# h3. imports
#
# Usual basic python module imports...
#
import os
import glob
import pprint
import socket
import datetime
import time
import json
import re
from configparser import ConfigParser
from sqlalchemy.inspection import inspect


# cherrypy and jinja imports...

import cherrypy
from jinja2 import Environment, PackageLoader
import jinja2.exceptions
import sqlalchemy.exc
import logging

# we import our logic modules, so we can attach an instance each to
# our overall poms_service class.

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

#
# ORM model is in source:poms_model.py
#

from .poms_model import Campaign, CampaignStage, Submission, Experiment, LoginSetup, Base
from .utc import utc

class JSONORMEncoder(json.JSONEncoder):

    def default(self, obj):

        if obj == datetime:
            return 'datetime'

        if isinstance(obj, Base):
            # smash ORM objects into dictionaries
            res = {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}
            # first put in relationship keys, but not loaded
            res.update( {c.key: None for c in inspect(obj).mapper.relationships } )
            # load specific relationships that won't cause cycles
            res.update( {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.relationships if c.key.find('experimenter') >= 0} )

            return res

        if isinstance(obj, datetime.datetime):
            return(obj.strftime("%Y-%m-%dT%H:%M:%S"))

        return super(JSONORMEncoder, self).default(obj)
#
# h3. Error handling
#
# we have a routine here we give to cherrypy to format errors
#


def error_response():
    dump = ""
    if cherrypy.config.get("dump", True):
        dump = cherrypy._cperror.format_exc()
    message = dump.replace('\n', '<br/>')
    jinja_env = Environment(
        loader=PackageLoader(
            'poms.webservice',
            'templates'))
    template = jinja_env.get_template('error_response.html')
    path = cherrypy.config.get("pomspath", "/poms")
    body = template.render(
        message=message,
        pomspath=path,
        dump=dump,
        version=global_version)
    cherrypy.response.status = 500
    cherrypy.response.headers['content-type'] = 'text/html'
    cherrypy.response.body = body.encode()
    logit.log(dump)

#
# h2. decorator to rewrite common errors for a better error page
#


def error_rewrite(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except TypeError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, repr(e))
        except KeyError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Missing form field: %s" % repr(e))
        except sqlalchemy.exc.DataError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Invalid argument: %s" % repr(e))
        except ValueError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Invalid argument: %s" % repr(e))
        except jinja2.exceptions.UndefinedError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Missing arguments")
        except:
            raise

    return wrapper

#
# h2. overall PomsService class
#

class PomsService:

    #
    # h3. More Error Handling
    #
    # cherrypy config bits for error handling
    #
    _cp_config = {'request.error_response': error_response,
                  'error_page.404': "%s/%s" % (os.path.abspath(os.getcwd()), 'templates/page_not_found.html'),
                  'error_page.401': "%s/%s" % (os.path.abspath(os.getcwd()), 'templates/unauthorized_user.html'),
                  'error_page.429': "%s/%s" % (os.path.abspath(os.getcwd()), 'templates/too_many.html'),
                  'error_page.400': "%s/%s" % (os.path.abspath(os.getcwd()), 'templates/bad_parameters.html'),
                  }


# h3. Module init
#
# we instantiate the logic modules and attach those instances here.
# as well as stashing some config values, etc. for later use.
#

    def __init__(self):
        ##
        # USE post_initialize if you need to log data!!!
        ##
        global global_version
        self.jinja_env = Environment(
            loader=PackageLoader(
                'poms.webservice',
                'templates'))
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
        self.filesPOMS = FilesPOMS.FilesStatus(self)
        self.tablesPOMS = None

    def post_initialize(self):
        # Anything that needs to log data must be called here -- after loggers
        # are configured.
        self.tablesPOMS = TablesPOMS.TablesPOMS(self)

#
# h2. web methods
#
# These are the actual methods cherrypy exposes as webservice endpoints.
# we use cherrypy and logit decorators  on pretty much all of them to
# make them visible etc, and a few that return JSON use extra decorators
#
# h4. headers

    @cherrypy.expose
    @logit.logstartstop
    def headers(self):
        return repr(cherrypy.request.headers)

# h4. sign_out
    @cherrypy.expose
    @logit.logstartstop
    def sign_out(self):
        cherrypy.lib.sessions.expire()
        log_out_url = "https://" + self.hostname + "/Shibboleth.sso/Logout"
        raise cherrypy.HTTPRedirect(log_out_url)

# h4. index
    @cherrypy.expose
    @logit.logstartstop
    def index(self):
        template = self.jinja_env.get_template('index.html')
        return template.render(services='',
                               version=self.version,
                               launches=self.taskPOMS.get_job_launches(
                                   cherrypy.request.db),
                               do_refresh=1200, help_page="DashboardHelp")

    ####################
    # UtilsPOMS

# h4. quick_search
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def quick_search(self, search_term):
        self.utilsPOMS.quick_search(cherrypy.HTTPRedirect, search_term)

# h4. update_session_experiment
    @cherrypy.expose
    @logit.logstartstop
    def update_session_experiment(self, *args, **kwargs):
        self.utilsPOMS.update_session_experiment(
            cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

# h4. update_session_role
    @cherrypy.expose
    @logit.logstartstop
    def update_session_role(self, *args, **kwargs):
        return self.utilsPOMS.update_session_role(
            cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

    #####
    # DBadminPOMS
# h4. raw_tables

    @cherrypy.expose
    @logit.logstartstop
    def raw_tables(self):
        if not cherrypy.session.get('experimenter').is_root():
            raise cherrypy.HTTPError(
                401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('raw_tables.html')
        return template.render(tlist=list(
            self.tablesPOMS.admin_map.keys()), help_page="RawTablesHelp")
#
# h4. experiment_list
#
# list of experiments we support for submission agent, etc to use.
#
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def experiment_list(self):
        return list(map((lambda x: x[0]), cherrypy.request.db.query(Experiment.experiment).all()))


# h4. experiment_membership


    @cherrypy.expose
    @logit.logstartstop
    def experiment_membership(self, *args, **kwargs):
        experiment = cherrypy.session.get('experimenter').session_experiment
        data = self.dbadminPOMS.experiment_membership(
            cherrypy.request.db, experiment, *args, **kwargs)
        template = self.jinja_env.get_template('experiment_membership.html')
        return template.render(data=data, help_page="MembershipHelp")

    # -----------------------------------------
    #################################
    # CampaignsPOMS

# h4. launch_template_edit
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def launch_template_edit(self, *args, **kwargs):
        # v3_2_0 backward compatabilty
        return self.login_setup_edit(*args, **kwargs)

# h4. login_setup_edit
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def login_setup_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.login_setup_edit(
            cherrypy.request.db, cherrypy.session.get, cherrypy.config.get, *args, **kwargs)

        if kwargs.get('test_template'):
            raise cherrypy.HTTPRedirect(
                "%s/launch_jobs?campaign_stage_id=None&test_login_setup=%s" % (self.path, data['login_setup_id']))

        template = self.jinja_env.get_template('login_setup_edit.html')
        return template.render(data=data, jquery_ui=False, help_page="LaunchTemplateEditHelp")


# h4. campaign_deps_ini


    @cherrypy.expose
    @logit.logstartstop
    def campaign_deps_ini(self, tag=None, camp_id=None, login_setup=None,
                          campaign_definition=None, launch_template=None, name=None, stage_id=None, job_type=None, full=None):
        experiment = cherrypy.session.get('experimenter').session_experiment
        res = self.campaignsPOMS.campaign_deps_ini(cherrypy.request.db, experiment,
                                                   name=name or tag,
                                                   stage_id=stage_id or camp_id,
                                                   login_setup=login_setup or launch_template,
                                                   job_type=job_type or campaign_definition, full=full)
        cherrypy.response.headers['Content-Type'] = 'text/ini'
        return res


# h4. campaign_deps


    @cherrypy.expose
    @logit.logstartstop
    def campaign_deps(self, campaign_name=None,
                      campaign_stage_id=None, tag=None, camp_id=None):

        template = self.jinja_env.get_template('campaign_deps.html')
        svgdata = self.campaignsPOMS.campaign_deps_svg(cherrypy.request.db, cherrypy.config.get,
                                                       campaign_name=campaign_name or tag,
                                                       campaign_stage_id=campaign_stage_id or camp_id)
        return template.render(tag=tag, svgdata=svgdata,
                               help_page="CampaignDepsHelp")


# h4. job_type_edit


    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def job_type_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.job_type_edit(
            cherrypy.request.db, cherrypy.session.get, *args, **kwargs)

        if kwargs.get('test_template'):
            test_campaign = self.campaignsPOMS.make_test_campaign_for(cherrypy.request.db, cherrypy.session,
                                                                      kwargs.get(
                                                                          "ae_campaign_definition_id"),
                                                                      kwargs.get("ae_definition_name"))
            raise cherrypy.HTTPRedirect(
                "%s/campaign_stage_edit?jump_to_campaign=%d&extra_edit_flag=launch_test_job" % (self.path, test_campaign))

        template = self.jinja_env.get_template('job_type_edit.html')
        return template.render(jquery_ui=False,
            data=data, help_page="CampaignDefinitionEditHelp")


# h4. make_test_campaign_for


    @cherrypy.expose
    @logit.logstartstop
    def make_test_campaign_for(self, campaign_def_id, campaign_def_name):
        cid = self.campaignsPOMS.make_test_campaign_for(cherrypy.request.db, cherrypy.session, campaign_def_id,
                                                        campaign_def_name)
        raise cherrypy.HTTPRedirect(
            "%s/campaign_stage_edit?campaign_stage_id=%d&extra_edit_flag=launch_test_job" % (self.path, cid))

    @cherrypy.expose
# h4. get_campaign_id
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def get_campaign_id(self, campaign_name):
        cid = self.campaignsPOMS.get_campaign_id(
            cherrypy.request.db, cherrypy.session.get, campaign_name)
        return cid

# h4. campaign_add_name
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_add_name(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_add_name(
            cherrypy.request.db, cherrypy.session.get, *args, **kwargs)
        return data

# h4. campaign_rename_name
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_rename_name(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_rename_name(
            cherrypy.request.db, cherrypy.session.get, *args, **kwargs)
        return data

# h4. campaign_stage_edit
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def campaign_stage_edit(self, *args, **kwargs):
        """
        :param args:
        :param kwargs:
        :return:
        """
        data = self.campaignsPOMS.campaign_stage_edit(
            cherrypy.request.db, cherrypy.session, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_stage_edit.html')

        if kwargs.get('pcl_call', '0') == '1':
            if data['message']:
                raise cherrypy.HTTPError(400, data['message'])

        if kwargs.get('launch_test_job', None) and kwargs.get(
                'ae_campaign_id', None):
            raise cherrypy.HTTPRedirect(
                "%s/launch_jobs?campaign_stage_id=%s" %
                (self.path, kwargs.get('ae_campaign_id')))

        return template.render(data=data, help_page="CampaignEditHelp",
                               jquery_ui=False,
                               extra_edit_flag=kwargs.get(
                                   "extra_edit_flag", None),
                               jump_to_campaign=kwargs.get(
                                   "jump_to_campaign", None)
                               )


# h4. gui_wf_edit


    @cherrypy.expose
    @logit.logstartstop
    def gui_wf_edit(self, *args, **kwargs):
        template = self.jinja_env.get_template('gui_wf_edit.html')
        return template.render(
            help_page="GUI_Workflow_Editor_User_Guide", campaign=kwargs.get('campaign'))


# h4. sample_workflows


    @cherrypy.expose
    @logit.logstartstop
    def sample_workflows(self, *args, **kwargs):
        sl = [
            x.replace(
                os.environ['POMS_DIR'] +
                '/webservice/static/',
                '') for x in glob.glob(
                os.environ['POMS_DIR'] +
                '/webservice/static/samples/*')]
        logit.log(
            "from %s think we got sl of %s" %
            (os.environ['POMS_DIR'], ",".join(sl)))
        template = self.jinja_env.get_template('sample_workflows.html')
        return template.render(help_page="Sample Workflows", sl=sl)


# h4. campaign_list_json


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_list_json(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_list(cherrypy.request.db)
        return data


# h4. campaign_stage_edit_query


    @cherrypy.expose
    @error_rewrite
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_stage_edit_query(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_stage_edit_query(
            cherrypy.request.db, *args, **kwargs)
        return data


# h4. new_task_for_campaign


    @cherrypy.expose
    @logit.logstartstop
    def new_task_for_campaign(
            self, campaign_name, command_executed, experimenter_name, dataset_name=None):
        return self.campaignsPOMS.new_task_for_campaign(cherrypy.request.db, campaign_name, command_executed,
                                                        experimenter_name, dataset_name)

# h4. show_campaigns
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def show_campaigns(self, *args, **kwargs):
        experimenter = cherrypy.session.get('experimenter')
        tl, last_activity, msg, data = self.campaignsPOMS.show_campaigns(
            cherrypy.request.db, cherrypy.session, experimenter, *args, **kwargs)
        template = self.jinja_env.get_template('show_campaigns.html')
        values =  { 'tl': tl, 'last_activity': last_activity, 'msg': msg, 'data': data, 'help_page': "ShowCampaignTagsHelp" }
        if kwargs.get('format','') == 'json':
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return json.dumps(values, cls=JSONORMEncoder).encode('utf-8')
        else:
            return template.render( **values )


# h4. show_campaign_stages


    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def show_campaign_stages(self, tmin=None, tmax=None, tdays=7, active=True, campaign_name=None, holder=None, role_held_with=None,
                             se_role=None, cl=None, **kwargs):
        (
            campaign_stages, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string, data
        ) = self.campaignsPOMS.show_campaign_stages(cherrypy.request.db,
                                                    tmin=tmin, tmax=tmax, tdays=tdays, active=active, campaign_name=campaign_name,
                                                    holder=holder, role_held_with=role_held_with,
                                                    campaign_ids=cl, sesshandler=cherrypy.session.get, **kwargs)

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
            template = self.jinja_env.get_template(
                'show_campaign_stages_stats.html')

        values = {
           'limit_experiment': current_experimenter.session_experiment,
           'campaign_stages': campaign_stages,
           'tmins': tmins, 'tmaxs': tmaxs,
           'tmin': str(tmin)[:16], 'tmax': str(tmax)[:16], 'tdays': tdays,
           'next': nextlink, 'prev': prevlink,
           'do_refresh': 1200,
           'data': data,
           'time_range_string': time_range_string,
           'key': '', 'help_page': "ShowCampaignsHelp", 'dbg': kwargs,
        }

        if kwargs.get('format','') == 'json':
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return json.dumps(values, cls=JSONORMEncoder).encode('utf-8')
        else:
            return template.render( **values )

# h4. reset_campaign_split


    @cherrypy.expose
    @logit.logstartstop
    def reset_campaign_split(self, campaign_stage_id):
        campaign = cherrypy.request.db.query(CampaignStage).filter(
            CampaignStage.campaign_stage_id == campaign_stage_id).first()
        if campaign and cherrypy.session.get(
                'experimenter').is_authorized(campaign):
            res = self.campaignsPOMS.reset_campaign_split(
                cherrypy.request.db,
                campaign_stage_id)
            raise cherrypy.HTTPRedirect(
                "campaign_stage_info?campaign_stage_id=%s" %
                campaign_stage_id)
        else:
            raise cherrypy.HTTPError(
                401, 'You are not authorized to access this resource')

# h4. campaign_stage_datasets
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_stage_datasets(self):
        return self.taskPOMS.campaign_stage_datasets(cherrypy.request.db)


# h4. submission_details
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def submission_details(self, submission_id, format = 'html'):
        submission, history, dataset, rmap, smap, ds, submission_log_format  = self.taskPOMS.submission_details(
            cherrypy.request.db, cherrypy.request.samweb_lite, cherrypy.HTTPError, cherrypy.config.get, submission_id)
        template = self.jinja_env.get_template('submission_details.html')
        values = {
            'submission': submission,
            'history': history,
            'dataset': dataset,
            'recoverymap': rmap,
            'statusmap': smap,
            'ds ':  ds,
            'submission_log_format ':  submission_log_format,
            'datetime': datetime,
            'do_refresh': 0,
            'help_page': "SubmissionDetailsHelp",
        }
        if format == 'json':
            cherrypy.response.headers['Content-Type'] = 'application/json'
            return json.dumps(values, cls=JSONORMEncoder).encode('utf-8')
        else:
            return template.render( **values )



# h4. campaign_stage_info


    @cherrypy.expose
    @cherrypy.tools.response_headers(headers=[('Content-Language', 'en')])
    @error_rewrite
    @logit.logstartstop
    def campaign_stage_info(self, campaign_stage_id,
                            tmin=None, tmax=None, tdays=None):
        (campaign_stage_info,
         time_range_string,
         tmins, tmaxs, tdays,
         campaign_definition_info, login_setup_info,
         campaigns,
         launched_campaigns, dimlist,
         campaign_stage,
         counts_keys, counts,
         launch_flist,
         kibana_link,
         dep_svg,
         last_activity,
         recent_submissions) = self.campaignsPOMS.campaign_stage_info(cherrypy.request.db,
                                                                                       cherrypy.config.get,
                                                                                       campaign_stage_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_stage_info.html')
        return template.render(
            Campaign_info=campaign_stage_info,
            time_range_string=time_range_string,
            tmins=tmins,
            tmaxs=tmaxs,
            Campaign_definition_info=campaign_definition_info,
            login_setup_info=login_setup_info,
            # campaigns=campaigns,      # Not used
            launched_campaigns=launched_campaigns,
            dimlist=dimlist,
            CampaignStage=campaign_stage,
            counts_keys=counts_keys,
            counts=counts,
            launch_flist=launch_flist,
            do_refresh=0,
            help_page="CampaignInfoHelp",
            kibana_link=kibana_link,
            dep_svg=dep_svg, last_activity=last_activity, recent_submissions = recent_submissions)

#   h4. campaign_stage_submissions
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def campaign_stage_submissions(self, campaign_name, stage_name, campaign_stage_id = None, campaign_id = None, tmin=None, tmax=None, tdays=1):
        data = self.campaignsPOMS.campaign_stage_submissions(cherrypy.request.db, campaign_name, stage_name, campaign_stage_id, campaign_id, tmin, tmax, tdays)
        data['campaign_name'] = campaign_name
        data['stage_name'] = stage_name
        template = self.jinja_env.get_template('campaign_stage_submissions.html')
        return template.render(data=data)

#   h4. session_status_history
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def session_status_history(self, submission_id):
        rows = self.campaignsPOMS.session_status_history(cherrypy.request.db, submission_id)
        return rows


# h4. register_poms_campaign


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

# h4. list_launch_file
    @cherrypy.expose
    @logit.logstartstop
    @error_rewrite
    @logit.logstartstop
    def list_launch_file(self, campaign_stage_id=None, fname=None,
                         login_setup_id=None, launch_template_id=None):
        if login_setup_id is None and launch_template_id is not None:
            login_setup_id = launch_template_id
        lines, refresh, campaign_name, stage_name = self.campaignsPOMS.list_launch_file(
            cherrypy.request.db, campaign_stage_id, fname, login_setup_id)
        output = "".join(lines)
        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command='', output=output, do_refresh=refresh,
                              cs=None, campaign_stage_id=campaign_stage_id,
                              campaign_name=campaign_name, stage_name=stage_name,
                              help_page="LaunchedJobsHelp")
        return res


# h4. schedule_launch


    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def schedule_launch(self, campaign_stage_id):
        cs, job, launch_flist = self.campaignsPOMS.schedule_launch(
            cherrypy.request.db, campaign_stage_id)
        template = self.jinja_env.get_template('schedule_launch.html')
        return template.render(cs=cs, campaign_stage_id=campaign_stage_id, job=job,
                               do_refresh=0, help_page="ScheduleLaunchHelp",
                               launch_flist=launch_flist)


# h4. update_launch_schedule


    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def update_launch_schedule(self, campaign_stage_id, dowlist=None, domlist=None,
                               monthly=None, month=None, hourlist=None, submit=None, minlist=None, delete=None):
        self.campaignsPOMS.update_launch_schedule(campaign_stage_id, dowlist, domlist, monthly, month, hourlist, submit,
                                                  minlist, delete, user=cherrypy.session.get('experimenter').experimenter_id)
        raise cherrypy.HTTPRedirect(
            "schedule_launch?campaign_stage_id=%s" %
            campaign_stage_id)


# h4. mark_campaign_active


    @cherrypy.expose
    @logit.logstartstop
    def mark_campaign_active(self, campaign_id=None, is_active="", cl=None):
        auth_error = self.campaignsPOMS.mark_campaign_active(
            campaign_id,
            is_active,
            cl,
            cherrypy.request.db,
            user=cherrypy.session.get('experimenter'))
        if auth_error:
            raise cherrypy.HTTPError(
                401, 'You are not authorized to access this resource')

# h4. mark_campaign_hold
    @cherrypy.expose
    @logit.logstartstop
    def mark_campaign_hold(self, ids2HR=None, is_hold=''):
        """
            Who can hold/release a campaign:
            The creator can hold/release her/his own campaign_stages.
            The root can hold/release any campaign_stages.
            The superuser can hold/release any campaign_stages that in the same experiment as the superuser.
            Anyone with a production role can hold/release a campaign created with a production role.

            :param  ids2HR: A list of campaign ids to be hold/released.
            :param is_hold: 'Hold'/'Queue' or 'Release'
            :return:
        """
        campaign_ids = ids2HR.split(",")
        sessionExperimenter = cherrypy.session.get('experimenter')
        for cid in campaign_ids:
            campaign = cherrypy.request.db.query(CampaignStage).filter(
                CampaignStage.campaign_stage_id == cid).first()
            if not campaign:
                raise cherrypy.HTTPError(
                    404, 'The campaign campaign_stage_id={} cannot be found.'.format(cid))
            mayIChangeIt = False
            if sessionExperimenter.is_root():
                mayIChangeIt = True
            elif sessionExperimenter.is_superuser() and sessionExperimenter.session_experiment == campaign.experiment:
                mayIChangeIt = True
            elif sessionExperimenter.is_production() and sessionExperimenter.session_experiment == campaign.experiment \
                    and campaign.creator_role == "production":
                mayIChangeIt = True
            elif campaign.creator == sessionExperimenter.experimenter_id and campaign.experiment == sessionExperimenter.session_experiment \
                    and campaign.creator_role == sessionExperimenter.session_role:
                mayIChangeIt = True
            else:
                raise cherrypy.HTTPError(
                    401, 'You are not authorized to hold or release this campaign_stages. ')

            if mayIChangeIt:
                if is_hold in ("Hold","Queue"):
                    campaign.hold_experimenter_id = sessionExperimenter.experimenter_id
                    campaign.role_held_with = sessionExperimenter.session_role
                elif is_hold == "Release":
                    campaign.hold_experimenter_id = None
                    campaign.role_held_with = None
                else:
                    raise cherrypy.HTTPError(
                        400, 'The action is not supported. You can only Hold/Queue or Release.')
                cherrypy.request.db.add(campaign)
                cherrypy.request.db.commit()
            else:
                raise cherrypy.HTTPError(
                    401, 'You are not authorized to hold or release this campaign_stages. ')

        if ids2HR:
            referrer = cherrypy.request.headers.get('Referer')
            if referrer:
                raise cherrypy.HTTPRedirect(referrer[referrer.rfind('/')+1:])

            raise cherrypy.HTTPRedirect("show_campaign_stages")


# h4. make_stale_campaigns_inactive


    @cherrypy.expose
    @logit.logstartstop
    def make_stale_campaigns_inactive(self):
        if not cherrypy.session.get('experimenter').is_root():
            raise cherrypy.HTTPError(
                401, 'You are not authorized to access this resource')
        res = self.campaignsPOMS.make_stale_campaigns_inactive(
            cherrypy.request.db, cherrypy.HTTPError)
        return "Marked inactive stale: " + ",".join(res)


# h4. list_generic


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

# h4. edit_screen_generic
    @cherrypy.expose
    @logit.logstartstop
    def edit_screen_generic(self, classname, id=None):
        return self.tablesPOMS.edit_screen_generic(cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session,
                                                   classname, id)

# h4. update_generic
    @cherrypy.expose
    @logit.logstartstop
    def update_generic(self, classname, *args, **kwargs):
        return self.tablesPOMS.update_generic(cherrypy.request.db, cherrypy.request.headers.get, cherrypy.session,
                                              classname, *args, **kwargs)

# h4. edit_screen_for
    @cherrypy.expose
    @logit.logstartstop
    def edit_screen_for(self, classname, eclass,
                        update_call, primkey, primval, valmap):
        screendata = self.tablesPOMS.edit_screen_for(cherrypy.request.db,
                                                     cherrypy.request.headers.get,
                                                     cherrypy.session, classname, eclass,
                                                     update_call, primkey, primval, valmap)
        template = self.jinja_env.get_template('edit_screen_for.html')
        return template.render(screendata=screendata, action="./" + update_call,
                               classname=classname,
                               help_page="GenericEditHelp")

    #######
    # JobPOMS

# h4. active_jobs
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def active_jobs(self):
        res = self.jobsPOMS.active_jobs(cherrypy.request.db)
        return list(res)


# h4. force_locate_submission


    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def force_locate_submission(self, submission_id):
        return self.taskPOMS.force_locate_submission(
            cherrypy.request.db, submission_id)

# h4. mark_failed_submissions
    @cherrypy.expose
    @logit.logstartstop
    def mark_failed_submissions(self):
        return self.taskPOMS.mark_failed_submissions(cherrypy.request.db)

# h4. running_submissions
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def running_submissions(self, campaign_id_list):
        cl = list(map(int, campaign_id_list.split(',')))
        return self.taskPOMS.running_submissions(cherrypy.request.db, cl)

# h4. update_submission
    @cherrypy.expose
    @logit.logstartstop
    def update_submission(self, submission_id, jobsub_job_id,
                          pct_complete=None, status=None, project=None):
        res = self.taskPOMS.update_submission(cherrypy.request.db, submission_id, jobsub_job_id,
                                              status=status, project=project, pct_complete=pct_complete)
        return res


# h4. json_pending_for_campaigns


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_pending_for_campaigns(self, cl, tmin, tmax, uuid=None):
        res = self.filesPOMS.get_pending_dict_for_campaigns(
            cherrypy.request.db, cherrypy.request.samweb_lite, cl, tmin, tmax)
        return res

# h3. File upload management for Analysis users
#
# h4. file_uploads
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def file_uploads(self):
        quota = cherrypy.config.get('base_uploads_quota')
        file_stat_list, total = self.filesPOMS.file_uploads(
            cherrypy.config.get('base_uploads_dir'),
            cherrypy.session.get,
            quota
        )

        template = self.jinja_env.get_template('file_uploads.html')
        return template.render(
            file_stat_list = file_stat_list,
            total = total,
            quota = quota,
            time = time)

# h4. upload_files
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def upload_file(self, filename):
        res = self.filesPOMS.upload_file(
            cherrypy.config.get('base_uploads_dir'),
            cherrypy.session.get,
            cherrypy.HTTPError,
            cherrypy.config.get('base_uploads_quota'),
            filename
        )
        raise cherrypy.HTTPRedirect(self.path + "/file_uploads")

# h4. remove_uploaded_files
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def remove_uploaded_files(self, filename, action):
        res = self.filesPOMS.remove_uploaded_files(
            cherrypy.config.get('base_uploads_dir'),
            cherrypy.session.get,
            cherrypy.HTTPError,
            filename,
            action
        )
        raise cherrypy.HTTPRedirect(self.path + "/file_uploads")


# h3. Job actions
#
# h4. kill_jobs


    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def kill_jobs(self, campaign_id = None, campaign_stage_id=None, submission_id=None,
                  task_id=None, job_id=None, confirm=None, act='kill'):
        if task_id is not None and submission_id is None:
            submission_id = task_id
        if confirm is None:
            what, s, campaign_stage_id, submission_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, cherrypy.session.get, campaign_id, campaign_stage_id, submission_id,
             job_id, confirm, act)
            template = self.jinja_env.get_template('kill_jobs_confirm.html')
            return template.render(what = what, task=s, campaign_stage_id=campaign_stage_id,
                                   submission_id=submission_id, job_id=job_id, act=act,
                                   help_page="KilledJobsHelp")

        else:
            output, cs, campaign_stage_id, submission_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, cherrypy.session.get, campaign_id, campaign_stage_id, submission_id,
                                                                                           job_id, confirm, act)
            template = self.jinja_env.get_template('kill_jobs.html')
            return template.render(output=output,
                                   cs=cs, campaign_stage_id=campaign_stage_id, submission_id=submission_id,
                                   job_id=job_id, act=act,
                                   help_page="KilledJobsHelp")


# h4. set_job_launches


    @cherrypy.expose
    @logit.logstartstop
    def set_job_launches(self, hold):
        self.taskPOMS.set_job_launches(cherrypy.request.db, cherrypy.session.get, hold)
        raise cherrypy.HTTPRedirect(self.path + "/")


# h4. launch_queued_job


    @cherrypy.expose
    @logit.logstartstop
    def launch_queued_job(self):
        return self.taskPOMS.launch_queued_job(cherrypy.request.db,
                                               cherrypy.request.samweb_lite,
                                               cherrypy.session,
                                               cherrypy.request.headers.get,
                                               cherrypy.session,
                                               cherrypy.response.status,
                                               cherrypy.config.get('base_uploads_dir')
                                               )
# h4. launch_campaign
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def launch_campaign(self, campaign_id=None, dataset_override=None, parent_submission_id=None, parent_task_id=None, test_login_setup=None,
                    experiment=None, launcher=None, test_launch=False, output_commands=None):
        if cherrypy.session.get('experimenter').username and (
                'poms' != cherrypy.session.get('experimenter').username or launcher == ''):
            launch_user = cherrypy.session.get('experimenter').experimenter_id
        else:
            launch_user = launcher

        logit.log( "calling launch_campaign with campaign_id='%s'" % campaign_id)

        vals = self.campaignsPOMS.launch_campaign(cherrypy.request.db,
                                         cherrypy.config.get,
                                         cherrypy.request.headers.get,
                                         cherrypy.session.get,
                                         cherrypy.request.samweb_lite,
                                         cherrypy.HTTPError,
                                         cherrypy.config.get('base_uploads_dir'),
                                         campaign_id,
                                         launch_user,
                                         experiment=experiment,
                                         test_launch=test_launch,
                                         output_commands=output_commands)

        logit.log("Got vals: %s" % repr(vals))

        if output_commands:
            cherrypy.response.headers['Content-Type'] = "text/plain"
            return vals

        lcmd, cs, campaign_stage_id, outdir, outfile = vals
        if lcmd == "":
            return "Launches held, job queued..."
        else:
            raise cherrypy.HTTPRedirect(
                    "%s/list_launch_file?campaign_stage_id=%s&fname=%s" % (self.path, campaign_stage_id, os.path.basename(outfile)))


# h4. test_split_type_editors

    @cherrypy.expose
    @logit.logstartstop
    def test_split_type_editors(self):
        template = self.jinja_env.get_template('test_split_type_editors.html')
        return template.render()

# h4. launch_jobs
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def launch_jobs(self, campaign_stage_id=None, dataset_override=None, parent_submission_id=None, parent_task_id=None, test_login_setup=None,
                    experiment=None, launcher=None, test_launch=False, test_launch_template=None, campaign_id=None, test=None, output_commands=None):
        if not campaign_stage_id and campaign_id:
            campaign_stage_id = campaign_id
        if not test_login_setup and test_launch_template:
            test_login_setup = test_launch_template
        if parent_task_id and not parent_submission_id:
            parent_submission_id = parent_task_id
        if cherrypy.session.get('experimenter').username and (
                'poms' != cherrypy.session.get('experimenter').username or not launcher ):
            launch_user = cherrypy.session.get('experimenter').experimenter_id
        else:
            launch_user = launcher

        logit.log(
            "calling launch_jobs with campaign_stage_id='%s'" %
            campaign_stage_id)

        vals = self.taskPOMS.launch_jobs(cherrypy.request.db,
                                         cherrypy.config.get,
                                         cherrypy.request.headers.get,
                                         cherrypy.session.get,
                                         cherrypy.request.samweb_lite,
                                         cherrypy.HTTPError,
                                         cherrypy.config.get('base_uploads_dir'),
                                         campaign_stage_id,
                                         launch_user,
                                         dataset_override=dataset_override,
                                         parent_submission_id=parent_submission_id, test_login_setup=test_login_setup,
                                         experiment=experiment,
                                         test_launch=test_launch,
                                         output_commands=output_commands)
        logit.log("Got vals: %s" % repr(vals))

        if output_commands:
            cherrypy.response.headers['Content-Type'] = "text/plain"
            return vals

        lcmd, cs, campaign_stage_id, outdir, outfile = vals
        if lcmd == "":
            return outfile
        else:
            if test_login_setup:
                raise cherrypy.HTTPRedirect("%s/list_launch_file?login_setup_id=%s&fname=%s" % (
                    self.path, test_login_setup, os.path.basename(outfile)))
            else:
                raise cherrypy.HTTPRedirect(
                    "%s/list_launch_file?campaign_stage_id=%s&fname=%s" % (self.path, campaign_stage_id, os.path.basename(outfile)))


# h4. launch_recovery_for


    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def launch_recovery_for(self, submission_id=None,
                            campaign_stage_id=None, recovery_type=None, launch=None):

        # we don't actually get the logfile, etc back from
        # launch_recovery_if_needed, so guess what it will be:

        s = cherrypy.request.db.query(Submission).filter(
            Submission.submission_id == submission_id).first()
        stime = datetime.datetime.now(utc)

        res = self.taskPOMS.launch_recovery_if_needed(
            cherrypy.request.db,
            cherrypy.request.samweb_lite,
            cherrypy.config.get,
            cherrypy.request.headers.get,
            cherrypy.session,
            cherrypy.HTTPError,
            s,
            recovery_type)

        if res:
            new = cherrypy.request.db.query(Submission).filter(
               Submission.recovery_tasks_parent == submission_id, 
               Submission.created >= stime).first()

            ds = new.created.strftime("%Y%m%d_%H%M%S")
            launcher_experimenter = new.experimenter_creator_obj
            outdir = "%s/private/logs/poms/launches/campaign_%s" % (
                os.environ["HOME"], campaign_stage_id)
            outfile = "%s/%s_%s_%s" % (outdir, ds, launcher_experimenter.username, new.submission_id)
            raise cherrypy.HTTPRedirect("%s/list_launch_file?campaign_stage_id=%s&fname=%s" % (
            self.path, campaign_stage_id, os.path.basename(outfile)))
        else:
            return "No recovery needed, launch skipped."

# h4. jobtype_list
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def jobtype_list(self, *args, **kwargs):
        data = self.jobsPOMS.jobtype_list(
            cherrypy.request.db, cherrypy.session.get)
        return data

    # ----------------------
    ########################
    # TaskPOMS

# h4. wrapup_tasks

    @cherrypy.expose
    @logit.logstartstop
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return "\n".join(self.taskPOMS.wrapup_tasks(cherrypy.request.db,
                                                    cherrypy.request.samweb_lite,
                                                    cherrypy.config.get,
                                                    cherrypy.request.headers.get,
                                                    cherrypy.session,
                                                    cherrypy.response.status,
                                                    cherrypy.config.get('base_uploads_dir')
                                                    ))


# h4. get_task_id_for


    @cherrypy.expose
    @logit.logstartstop
    def get_task_id_for(self, campaign, user=None, experiment=None, command_executed="", input_dataset="",
                        parent_task_id=None, task_id=None, parent_submission_id=None, submission_id=None, campaign_id=None, test=None):
        if not campaign and campaign_id:
            campaign = campaign_id
        if task_id is not None and submission_id is None:
            submission_id = task_id
        if parent_task_id is not None and parent_submission_id is None:
            parent_submission_id = parent_task_id
        submission_id = self.taskPOMS.get_task_id_for(cherrypy.request.db, campaign, user,
                                                      experiment, command_executed, input_dataset, parent_submission_id, submission_id)
        return "Task=%d" % submission_id

# h4. campaign_task_files

    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def campaign_task_files(self, campaign_stage_id=None,
                            campaign_id=None, tmin=None, tmax=None, tdays=1):
        (cs, columns, datarows,
         tmins, tmaxs,
         prevlink, nextlink, tdays) = self.filesPOMS.campaign_task_files(cherrypy.request.db,
                                                                         cherrypy.request.samweb_lite, campaign_stage_id, campaign_id,
                                                                         tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_task_files.html')
        return template.render(name=cs.name if cs else "",
                               CampaignStage = cs,
                               columns=columns, datarows=datarows,
                               tmin=tmins, tmax=tmaxs,
                               prev=prevlink, next=nextlink, tdays=tdays,
                               campaign_stage_id=campaign_stage_id, help_page="CampaignTaskFilesHelp")


# h4. show_dimension_files


    @cherrypy.expose
    @logit.logstartstop
    def show_dimension_files(self, experiment, dims):
        flist = self.filesPOMS.show_dimension_files(cherrypy.request.samweb_lite, experiment, dims,
                                                    dbhandle=cherrypy.request.db)
        template = self.jinja_env.get_template('show_dimension_files.html')
        return template.render(flist=flist, dims=dims, statusmap=[
        ], help_page="ShowDimensionFilesHelp")


# h4. actual_pending_files


    @cherrypy.expose
    @logit.logstartstop
    def actual_pending_files(
            self, count_or_list=None, campaign_stage_id=None, tmin=None, tmax=None, tdays=1):
        exps, dims = self.filesPOMS.actual_pending_file_dims(cherrypy.request.db,
                                                             cherrypy.request.samweb_lite,
                                                             campaign_stage_id=campaign_stage_id,
                                                             tmin=tmin, tmax=tmax, tdays=tdays)
        return self.show_dimension_files(exps[0], dims[0])


# h4. json_project_summary_for_task
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def json_project_summary_for_task(self, submission_id=None, task_id=None):
        if task_id is not None and submission_id is None:
            submission_id = task_id
        return self.project_summary_for_task(submission_id)
# h4. project_summary_for_task

    def project_summary_for_task(self, submission_id=None, task_id=None):
        if task_id is not None and submission_id is None:
            submission_id = task_id
        s = cherrypy.request.db.query(Submission).filter(
            Submission.submission_id == submission_id).first()
        return cherrypy.request.samweb_lite.fetch_info(s.campaign_stage_snapshot_obj.experiment, s.project,
                                                       # h4. project_summary_for_tasks
                                                       dbhandle=cherrypy.request.db)

    def project_summary_for_tasks(self, task_list):
        return cherrypy.request.samweb_lite.fetch_info_list(
            task_list, dbhandle=cherrypy.request.db)
    ##############
    # TagsPOMS

# h4. link_tags
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def link_tags(self, campaign_id=None, tag_name=None, experiment=None):
        return self.tagsPOMS.link_tags(cherrypy.request.db, cherrypy.session.get,
                                       campaign_id=campaign_id, tag_name=tag_name, experiment=experiment)


# h4. delete_campaigns_tags


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def delete_campaigns_tags(
            self, campaign_id, tag_id, experiment, delete_unused_tag=False):
        return self.tagsPOMS.delete_campaigns_tags(cherrypy.request.db, cherrypy.session.get, campaign_id, tag_id,
                                                   experiment, delete_unused_tag)

# h4. search_tags
    @cherrypy.expose
    @logit.logstartstop
    def search_tags(self, q):
        results = self.tagsPOMS.search_tags(cherrypy.request.db, tag_name=q)
        template = self.jinja_env.get_template('search_tags.html')
        return template.render(
            results=results, search_term=q, do_refresh=0, help_page="SearchTagsHelp")


# h4. search_campaigns


    @cherrypy.expose
    @logit.logstartstop
    def search_campaigns(self, search_term):
        results = self.tagsPOMS.search_campaigns(
            cherrypy.request.db, search_term)
        template = self.jinja_env.get_template('search_campaigns.html')
        return template.render(
            results=results, search_term=search_term, do_refresh=0, help_page="SearchTagsHelp")


# h4. search_all_tags


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def search_all_tags(self, cl):
        return self.tagsPOMS.search_all_tags(cherrypy.request.db, cl)


# h4. auto_complete_tags_search


    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def auto_complete_tags_search(self, experiment, q):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return self.tagsPOMS.auto_complete_tags_search(
            cherrypy.request.db, experiment, q)

    # -----------------------
    # debugging

# h4. echo
    @cherrypy.expose
    @cherrypy.tools.json_out()
    # @cherrypy.tools.json_in()
    def echo(self, *args, **kwargs):
        data = self.campaignsPOMS.echo(
            cherrypy.request.db, cherrypy.session, *args, **kwargs)
        return data

# h4. split_type_javascript
    @cherrypy.expose
    @cherrypy.tools.response_headers(headers=[('Content-Type', 'text/javascript')])
    def split_type_javascript(self, *args, **kwargs):
        data = self.campaignsPOMS.split_type_javascript(
            cherrypy.request.db, cherrypy.session, cherrypy.request.samweb_lite,*args, **kwargs)
        return data

# h4. save_campaign
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def save_campaign(self, *args, **kwargs):
        data = self.campaignsPOMS.save_campaign(
            cherrypy.request.db, cherrypy.session, *args, **kwargs)
        return data

# h4. get_jobtype_id
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_jobtype_id(self, name):
        data = self.campaignsPOMS.get_jobtype_id(
            cherrypy.request.db, cherrypy.session, name)
        return data

# h4. get_loginsetup_id
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_loginsetup_id(self, name):
        data = self.campaignsPOMS.get_loginsetup_id(
            cherrypy.request.db, cherrypy.session, name)
        return data

# h4. loginsetup_list
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def loginsetup_list(self, name=None, full=None, **kwargs):
        exp = cherrypy.session.get('experimenter').session_experiment
        if full:
            data = cherrypy.request.db.query(LoginSetup.name,
                                             LoginSetup.launch_host,
                                             LoginSetup.launch_account,
                                             LoginSetup.launch_setup).filter(LoginSetup.experiment == exp).order_by(LoginSetup.name).all()
        else:
            data = cherrypy.request.db.query(
                LoginSetup.name).filter(
                LoginSetup.experiment == exp).order_by(
                LoginSetup.name).all()

        return [r._asdict() for r in data]


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def ini_to_campaign(self, upload=None, **kwargs):
        # import pprint
        if not upload.file:
            return "Pick the file first!"

        print("upload='{}', kwargs={}".format(upload, kwargs))
        print("fn: {}, ct: {}, {}".format(upload.filename, upload.content_type, upload.file))
        data = upload.file.read().decode("utf-8")
        # print("{}".format(data))

        p = ConfigParser(interpolation=None)
        try:
            p.read_string(data)
        except Exception:
            return {'status': "400 Bad Request", 'message': "Bad file format"}

        cfg = {section: dict(p.items(section)) for section in p.sections()}
        pprint.pprint(cfg)

        # Now reformat into JSON suitable for save_campaign call
        campaign_d = {"stages": [], "dependencies": [], "misc": []}

        campaign_s = cfg.get('campaign')
        if campaign_s is None:
            return {'status': "400 Bad Request", 'message': "No Campaign section in the file"}

        camp_name = campaign_s['name']
        campaign_d["stages"].append(
            {
                "id": "campaign {}".format(camp_name),
                "label": camp_name,
                "clean": False,
                "form": cfg.get('campaign_defaults', {}),
            })
        # Process stages
        stage_names = [k for k in cfg if k.startswith('campaign_stage ')]
        if not stage_names:
            return {'status': "400 Bad Request", 'message': "No Campaign Stage sections in the file"}

        for name in stage_names:
            sn = name.split(' ', 1)[1]
            campaign_d["stages"].append({"id": sn, "label": sn, "clean": False, "form": cfg[name]})
        # Process dependencies
        dep_names = [k for k in cfg if k.startswith('dependencies ')]
        for name in dep_names:
            section = cfg[name]
            sn = name.split(' ', 1)[1]
            from_ids = [k for k in section if k.startswith("campaign_stage_")]
            for from_id in from_ids:
                sfx = from_id.rsplit("_", 1)[1]
                sel = f"file_pattern_{sfx}"
                campaign_d["dependencies"].append(
                    {
                        "id": f"{name}_{sfx}",
                        "fromId": section[from_id],
                        "toId": sn,
                        "clean": False,
                        "form": {sel: section[sel]},
                    })
        # Process JobTypes and LoginSetups
        misc_names = [k for k in cfg if k.startswith('job_type ') or k.startswith('login_setup ')]
        for name in misc_names:
            section = cfg[name]
            sn = name.split(' ', 1)[1]
            campaign_d["misc"].append({"id": name, "label": sn, "clean": False, "form": section})
        # Save the campaign
        data = self.campaignsPOMS.save_campaign(cherrypy.request.db, cherrypy.session, form=json.dumps(campaign_d), **kwargs)

        return data
