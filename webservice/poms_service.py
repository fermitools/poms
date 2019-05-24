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
import logging
from configparser import ConfigParser

import cherrypy
from jinja2 import Environment, PackageLoader
import jinja2.exceptions
import sqlalchemy.exc
from sqlalchemy.inspection import inspect
from .get_user import get_user
from .poms_method import poms_method, error_rewrite

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
    Permissions,
    logit,
    version,
)

#
# ORM model is in source:poms_model.py
#

from .poms_model import CampaignStage, Submission, Experiment, LoginSetup, Base, Experimenter
from .utc import utc

from .Ctx import Ctx

#
# h3. Error handling
#
# we have a routine here we give to cherrypy to format errors
#


def error_response():
    dump = ""
    if cherrypy.config.get("dump", True):
        dump = cherrypy._cperror.format_exc()
    message = dump.replace("\n", "<br/>")
    jinja_env = Environment(loader=PackageLoader("poms.webservice", "templates"))
    template = jinja_env.get_template("error_response.html")
    path = cherrypy.config.get("pomspath", "/poms")
    body = template.render(message=message, pomspath=path, dump=dump, version=global_version)
    cherrypy.response.status = 500
    cherrypy.response.headers["content-type"] = "text/html"
    cherrypy.response.body = body.encode()
    logit.log(dump)


#
# h2. overall PomsService class
#


class PomsService:

    #
    # h3. More Error Handling
    #
    # cherrypy config bits for error handling
    #
    tdir = os.environ["POMS_DIR"] + "/webservice/templates"
    _cp_config = {
        "request.error_response": error_response,
        "error_page.400": "%s/%s" % (tdir, "bad_parameters.html"),
        "error_page.401": "%s/%s" % (tdir, "unauthorized_user.html"),
        "error_page.404": "%s/%s" % (tdir, "page_not_found.html"),
        "error_page.429": "%s/%s" % (tdir, "too_many.html"),
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
        self.jinja_env = Environment(loader=PackageLoader("poms.webservice", "templates"))
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
        self.permissions = Permissions.Permissions()

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

    @poms_method()
    def headers(self, **kwargs):
        return repr(cherrypy.request.headers)

    # h4. sign_out
    @poms_method(rtype="redirect", redirect="https://%(hostname)s/Shibboleth.sso/Logout")
    def sign_out(self, **kwargs):
        pass

    # h4. index
    @poms_method(t="index.html", help_page="DashboardHelp")
    def index(self, **kwargs):
        if len(cherrypy.request.path_info.split("/")) < 3:
            experiment, role = self.utilsPOMS.getSavedExperimentRole(kwargs["ctx"])
            raise cherrypy.HTTPRedirect("%s/index/%s/%s" % (self.path, experiment, role))
        return {"version": self.version, "launches": self.taskPOMS.get_job_launches(kwargs["ctx"])}

    ####################
    # UtilsPOMS

    # h4. quick_search
    @poms_method()
    def quick_search(self, **kwargs):
        return self.utilsPOMS.quick_search(cherrypy.HTTPRedirect, **kwargs)

    # h4. update_session_experiment
    @poms_method()
    def update_session_experiment(self, **kwargs):
        self.utilsPOMS.update_session_experiment(**kwargs)
        raise cherrypy.HTTPRedirect(
            kwargs["ctx"]
            .headers_get("Referer", "%s/index/%s/%s" % (self.path, kwargs["ctx"].experiment, kwargs["ctx"].role))
            .replace(kwargs["ctx"].experiment, kwargs["session_experiment"])
        )

    # h4. update_session_role
    @poms_method()
    def update_session_role(self, **kwargs):
        self.utilsPOMS.update_session_role(**kwargs)
        raise cherrypy.HTTPRedirect(
            kwargs["ctx"]
            .headers_get("Referer", "%s/index/%s/%s" % (self.path, kwargs["ctx"].experiment, kwargs["ctx"].role))
            .replace(kwargs["ctx"].role, kwargs["session_role"])
        )

    #####
    # DBadminPOMS
    # h4. raw_tables

    @poms_method(p=[{"p": "is_superuser"}], t="raw_tables.html", help_page="RawTablesHelp")
    def raw_tables(self):
        return {"tlist": list(self.tablesPOMS.admin_map.keys())}

    #
    # h4. experiment_list
    #
    # list of experiments we support for submission agent, etc to use.
    #
    @poms_method(rtype="json")
    def experiment_list(self, **kwargs):
        return list(
            map((lambda x: x[0]), kwargs["ctx"].db.query(Experiment.experiment).filter(Experiment.active.is_(True)).all())
        )

    # h4. experiment_membership

    @poms_method(
        p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}],
        help_page="MembershipHelp",
        t="experiment_membership.html",
    )
    def experiment_membership(self, **kwargs):
        return {"data": self.dbadminPOMS.experiment_membership(**kwargs)}

    # -----------------------------------------
    #################################
    # CampaignsPOMS

    # h4. launch_template_edit
    @poms_method()
    def launch_template_edit(self, **kwargs):
        return self.login_setup_edit(**kwargs)

    # h4. login_setup_rm
    @poms_method(rtype="json")
    def login_setup_rm(self, **kwargs):
        # note: login_setup_edit checks for delete permission...
        kwargs["action"] = "delete"
        return self.campaignsPOMS.login_setup_edit(**kwargs)

    # h4. login_setup_edit
    @poms_method(p=[{"p": "can_modify", "t": "LoginSetup", "name": "ae_launch_name"}], t="login_setup_edit.html")
    def login_setup_edit(self, **kwargs):
        res = {"data": self.campaignsPOMS.login_setup_edit(**kwargs)}
        if kwargs.get("test_template"):
            raise cherrypy.HTTPRedirect(
                "%s/launch_jobs/%s/%s?campaign_stage_id=None&test_login_setup=%s"
                % (self.path, experiment, role, data["login_setup_id"])
            )
        return res

    # h4. campaign_deps_ini

    @poms_method(
        p=[{"p": "can_view", "t": "Campaign", "item_id": "camp_id"}, {"p": "can_view", "t": "Campaign", "item_id": "stage_id"}],
        rtype="ini",
    )
    def campaign_deps_ini(self, **kwargs):
        return self.campaignsPOMS.campaign_deps_ini(**kwargs)

    # h4. campaign_deps

    @poms_method(
        p=[{"p": "can_view", "t": "Campaign", "name": "campaign_name"}], t="campaign_deps.html", help_page="CampaignDepsHelp"
    )
    def campaign_deps(self, **kwargs):
        return {"svgdata": self.campaignsPOMS.campaign_deps_svg(**kwargs)}

    # h4. job_type_rm
    @poms_method(rtype="json")
    def job_type_rm(self, **kwargs):
        return self.campaignsPOMS.job_type_edit(action="delete", **kwargs)

    # h4. job_type_edit

    @poms_method(p=[{"p": "can_modify", "t": "LoginSetup", "name": "ae_launch_name"}], t="job_type_edit.html")
    def job_type_edit(self, **kwargs):
        res = {"data": self.campaignsPOMS.job_type_edit(**kwargs), "jquery_ui": False}
        if kwargs.get("test_template"):
            test_campaign = self.campaignsPOMS.make_test_campaign_for(
                kwargs["ctx"], kwargs.get("ae_campaign_definition_id"), kwargs.get("ae_definition_name")
            )
            raise cherrypy.HTTPRedirect(
                "%s/%s/%s/campaign_stage_edit?jump_to_campaign=%d&extra_edit_flag=launch_test_job"
                % (self.path, experiment, role, test_campaign)
            )
        return res

    # h4. make_test_campaign_for

    @poms_method(p=[{"p": "can_modify", "t": "JobType", "name": "campaign_def_name", "experiment": "experiment"}])
    def make_test_campaign_for(self, **kwargs):
        cid = self.campaignsPOMS.make_test_campaign_for(
            ctx.db, ctx.username, experiment, role, campaign_def_id, campaign_def_name
        )
        raise cherrypy.HTTPRedirect(
            "%s/campaign_stage_edit/%s/%s?campaign_stage_id=%d&extra_edit_flag=launch_test_job"
            % (self.path, experiment, role, cid)
        )

    # h4. get_campaign_id

    @poms_method(rtype="json")
    def get_campaign_id(self, **kwargs):
        return self.campaignsPOMS.get_campaign_id(ctx, campaign_name)

    # h4. get_campaign_name

    @poms_method()
    def get_campaign_name(self, **kwargs):
        return self.campaignsPOMS.get_campaign_name(**kwargs)

    # h4. get_campaign_stage_name

    @poms_method()
    def get_campaign_stage_name(self, **kwargs):
        return self.campaignsPOMS.get_campaign_stage_name(**kwargs)

    # h4. campaign_add_name
    @poms_method()
    def campaign_add_name(self, **kwargs):
        return self.campaignsPOMS.campaign_add_name(**kwargs)

    # h4. campaign_stage_edit
    @poms_method(
        p=[{"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        help_page="POMS_User_Documentation",
        t="campaign_stage_edit.html",
    )
    def campaign_stage_edit(self, **kwargs):
        data = self.campaignsPOMS.campaign_stage_edit(**kwargs)
        if kwargs.get("pcl_call", "0") == "1" and data["message"]:
            raise cherrypy.HTTPError(400, data["message"])

        if kwargs.get("launch_test_job", None) and kwargs.get("ae_campaign_id", None):
            raise cherrypy.HTTPRedirect(
                "%s/%s/%s/launch_jobs?campaign_stage_id=%s" % (self.path, experiment, role, kwargs.get("ae_campaign_id"))
            )

        return {"data": data, "jquery_ui": False}

    # h4. gui_wf_edit

    @poms_method(
        p=[{"p": "can_modify", "t": "Campaign", "name": "campaign", "experiment": "experiment"}],
        t="gui_wf_edit.html",
        help_page="GUI_Workflow_Editor_User_Guide",
    )
    def gui_wf_edit(self, experiment, role, *args, **kwargs):
        return {}

    # h4. sample_workflows

    @poms_method(t="sample_workflows.html", help_page="Sample Workflows")
    def sample_workflows(self, **kwargs):
        import mimetypes

        mimetypes.types_map[".ini"] = "text/plain"

        return {
            "sl": [
                x.replace(os.environ["POMS_DIR"] + "/webservice/static/", "")
                for x in glob.glob(os.environ["POMS_DIR"] + "/webservice/static/samples/*")
            ]
        }

    # h4. campaign_list_json

    @poms_method(rtype=json)
    def campaign_list_json(self, **kwargs):
        return self.campaignsPOMS.campaign_list(ctx)

    # h4. campaign_stage_edit_query

    @poms_method(
        rtype=json,
        p=[
            {"p": "can_view", "t": "LaunchSetup", "item_id": "ae_launch_id"},
            {"p": "can_view", "t": "JobType", "item_id": "ae_campaign_definition_id"},
        ],
    )
    def campaign_stage_edit_query(self, **kwargs):
        return self.campaignsPOMS.campaign_stage_edit_query(**kwargs)

    # h4. show_campaigns

    @poms_method(
        p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}],
        t="show_campaigns.html",
        u=["tl", "last_activity", "msg", "data"],
    )
    def show_campaigns(self, **kwargs):
        return self.campaignsPOMS.show_campaigns(**kwargs)

    # h4. show_campaign_stages

    @poms_method(
        u=[
            "campaign_stages",
            "tmin",
            "tmax",
            "tmins",
            "tmaxs",
            "tdays",
            "nextlink",
            "prevlink",
            "time_range_string",
            "data",
            "template",
            "limit_experiment",
            "key",
        ]
    )
    def show_campaign_stages(self, **kwargs):
        if kwargs.get("campaign_ids", None) is None:
            template = "show_campaign_stages.html"
        else:
            template = "show_campaign_stages_stats.html"
        return self.campaignsPOMS.show_campaign_stages(**kwargs) + (template, kwargs["ctx"].experiment, "")

    # h4. reset_campaign_split

    @poms_method(
        p=[{"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage"}],
        redirect="%(poms_path)s/campaign_stage_info/%(experiment)s/%(role)s?campaign_stage_id=%(campaign_stage_id)s",
        rtype="redirect",
    )
    def reset_campaign_split(self, **kwargs):
        return self.campaignsPOMS.reset_campaign_split(**kwargs)

    # h4. campaign_stage_datasets
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def campaign_stage_datasets(self):
        ctx = Ctx(experiment=experiment, role=role)
        return self.taskPOMS.campaign_stage_datasets(ctx.db)

    # h4. submission_details
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def submission_details(self, experiment, role, submission_id, fmt="html"):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_view(ctx, "Submission", item_id=submission_id)
        submission, history, dataset, rmap, smap, ds, submission_log_format = self.taskPOMS.submission_details(ctx, submission_id)
        template = self.jinja_env.get_template("submission_details.html")
        values = {
            "experiment": experiment,
            "role": role,
            "submission": submission,
            "history": history,
            "dataset": dataset,
            "recoverymap": rmap,
            "statusmap": smap,
            "ds": ds,
            "submission_log_format": submission_log_format,
            "datetime": datetime,
            "do_refresh": 0,
            "help_page": "SubmissionDetailsHelp",
        }
        if fmt == "json":
            cherrypy.response.headers["Content-Type"] = "application/json"
            return json.dumps(values, cls=JSONORMEncoder).encode("utf-8")
        else:
            return template.render(**values)

    # h4. campaign_stage_info

    @cherrypy.expose
    @cherrypy.tools.response_headers(headers=[("Content-Language", "en")])
    @error_rewrite
    @logit.logstartstop
    def campaign_stage_info(self, experiment, role, campaign_stage_id, tmin=None, tmax=None, tdays=None):
        ctx = Ctx(experiment=experiment, role=role, tmin=tmin, tmax=tmax, tdays=tdays)
        self.permissions.can_view(ctx, "CampaignStage", item_id=campaign_stage_id)
        (
            campaign_stage_info,
            time_range_string,
            tmins,
            tmaxs,
            tdays,
            campaign_definition_info,
            login_setup_info,
            campaigns,
            launched_campaigns,
            dimlist,
            campaign_stage,
            counts_keys,
            counts,
            launch_flist,
            kibana_link,
            dep_svg,
            last_activity,
            recent_submissions,
        ) = self.campaignsPOMS.campaign_stage_info(ctx, campaign_stage_id)
        template = self.jinja_env.get_template("campaign_stage_info.html")

        # mvi point to new POMS doc

        return template.render(
            experiment=experiment,
            role=role,
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
            help_page="POMS_UserDocumentation",
            kibana_link=kibana_link,
            dep_svg=dep_svg,
            last_activity=last_activity,
            recent_submissions=recent_submissions,
        )

    #   h4. campaign_stage_submissions
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def campaign_stage_submissions(
        self,
        experiment,
        role,
        campaign_name,
        stage_name,
        campaign_stage_id=None,
        campaign_id=None,
        tmin=None,
        tmax=None,
        tdays=None,
        **kwargs,
    ):
        ctx = Ctx(experiment=experiment, role=role, tmin=tmin, tmax=tmax, tdays=tdays)
        self.permissions.can_view(ctx, "CampaignStage", item_id=campaign_stage_id)
        self.permissions.can_view(ctx, "Campaign", item_id=campaign_id)
        data = self.campaignsPOMS.campaign_stage_submissions(ctx, campaign_name, stage_name, campaign_stage_id, campaign_id)
        data["campaign_name"] = campaign_name
        data["stage_name"] = stage_name
        template = self.jinja_env.get_template("campaign_stage_submissions.html")
        if kwargs.get("fmt", "") == "json":
            cherrypy.response.headers["Content-Type"] = "application/json"
            return json.dumps(data, cls=JSONORMEncoder).encode("utf-8")
        else:
            return template.render(experiment=experiment, role=role, data=data)

    #   h4. session_status_history
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def session_status_history(self, submission_id):
        ctx = Ctx()
        # we would take experiment and role, and check,but we do not really
        # care who sees submission history timelines..
        # self.permissions.can_view(ctx, "Submission", item_id=submission_id)
        rows = self.campaignsPOMS.session_status_history(ctx, submission_id)
        return rows

    # h4. list_launch_file
    @cherrypy.expose
    @logit.logstartstop
    @error_rewrite
    @logit.logstartstop
    def list_launch_file(
        self, experiment, role, campaign_stage_id=None, fname=None, login_setup_id=None, launch_template_id=None
    ):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_view(ctx, "CampaignStage", item_id=campaign_stage_id)
        if login_setup_id is None and launch_template_id is not None:
            login_setup_id = launch_template_id
        lines, refresh, campaign_name, stage_name = self.campaignsPOMS.list_launch_file(
            ctx, campaign_stage_id, fname, login_setup_id
        )
        output = "".join(lines)
        template = self.jinja_env.get_template("launch_jobs.html")
        res = template.render(
            command="",
            output=output,
            do_refresh=refresh,
            experiment=experiment,
            role=role,
            cs=None,
            campaign_stage_id=campaign_stage_id,
            campaign_name=campaign_name,
            stage_name=stage_name,
            help_page="LaunchedJobsHelp",
        )
        return res

    # h4. schedule_launch

    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def schedule_launch(self, experiment, role, campaign_stage_id):
        ctx = Ctx(experiment=experiment, role=role)
        cs, job, launch_flist = self.campaignsPOMS.schedule_launch(ctx, campaign_stage_id)
        self.permissions.can_modify(ctx, "CampaignStage", item_id=campaign_stage_id)
        template = self.jinja_env.get_template("schedule_launch.html")
        return template.render(
            cs=cs,
            campaign_stage_id=campaign_stage_id,
            job=job,
            do_refresh=0,
            help_page="ScheduleLaunchHelp",
            launch_flist=launch_flist,
        )

    # h4. update_launch_schedule

    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def update_launch_schedule(
        self,
        experiment,
        role,
        campaign_stage_id,
        dowlist=None,
        domlist=None,
        monthly=None,
        month=None,
        hourlist=None,
        submit=None,
        minlist=None,
        delete=None,
    ):
        self.permissions.can_modify(ctx, "CampaignStage", item_id=campaign_stage_id)
        self.campaignsPOMS.update_launch_schedule(
            ctx, campaign_stage_id, dowlist, domlist, monthly, month, hourlist, submit, minlist, delete, user=ctx.username
        )
        raise cherrypy.HTTPRedirect("schedule_launch/%s/%s?campaign_stage_id=%s" % experiment, role, campaign_stage_id)

    # h4. mark_campaign_active

    @cherrypy.expose
    @logit.logstartstop
    def mark_campaign_active(self, experiment, role, campaign_id=None, is_active="", cl=None):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_modify(ctx, "Campaign", item_id=campaign_id)
        self.campaignsPOMS.mark_campaign_active(ctx, campaign_id, is_active, cl)

    # h4. mark_campaign_hold
    @cherrypy.expose
    @logit.logstartstop
    def mark_campaign_hold(self, experiment, role, ids2HR=None, is_hold=""):
        ctx = Ctx(experiment=experiment, role=role)
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
        sessionExperimenter = cherrypy.session.get("experimenter")
        for cid in campaign_ids:
            campaign = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == cid).first()
            if not campaign:
                raise cherrypy.HTTPError(404, "The campaign campaign_stage_id={} cannot be found.".format(cid))
            mayIChangeIt = False
            if sessionExperimenter.is_root():
                mayIChangeIt = True
            elif sessionExperimenter.is_superuser() and experiment == campaign.experiment:
                mayIChangeIt = True
            elif (
                sessionExperimenter.is_production()
                and experiment == campaign.experiment
                and campaign.creator_role == "production"
            ):
                mayIChangeIt = True
            elif (
                campaign.creator == sessionExperimenter.experimenter_id
                and campaign.experiment == experiment
                and campaign.creator_role == role
            ):
                mayIChangeIt = True
            else:
                raise cherrypy.HTTPError(401, "You are not authorized to hold or release this campaign_stages. ")

            if mayIChangeIt:
                if is_hold in ("Hold", "Queue"):
                    campaign.hold_experimenter_id = sessionExperimenter.experimenter_id
                    campaign.role_held_with = role
                elif is_hold == "Release":
                    campaign.hold_experimenter_id = None
                    campaign.role_held_with = None
                else:
                    raise cherrypy.HTTPError(400, "The action is not supported. You can only Hold/Queue or Release.")
                ctx.db.add(campaign)
                ctx.db.commit()
            else:
                raise cherrypy.HTTPError(401, "You are not authorized to hold or release this campaign_stages. ")

        if ids2HR:
            referer = ctx.headers_get("Referer")
            if referer:
                raise cherrypy.HTTPRedirect(referer[referer.rfind("/") + 1 :])

            raise cherrypy.HTTPRedirect("show_campaign_stages/%s/%s" % (experiment, role))

    # h4. make_stale_campaigns_inactive

    @cherrypy.expose
    @logit.logstartstop
    def make_stale_campaigns_inactive(self):
        ctx = Ctx(experiment=experiment, role=role)
        if not self.permissions.is_superuser(ctx):
            raise cherrypy.HTTPError(401, "You are not authorized to access this resource")
        res = self.campaignsPOMS.make_stale_campaigns_inactive(ctx)
        return "Marked inactive stale: " + ",".join(res)

    # h4. list_generic

    @cherrypy.expose
    @logit.logstartstop
    def list_generic(self, classname):
        ctx = Ctx(experiment=experiment, role=role)
        if not self.permissions.is_superuser(ctx.db, ctx.username):
            raise cherrypy.HTTPError(401, "You are not authorized to access this resource")
        l = self.tablesPOMS.list_generic(ctx.db, classname)
        template = self.jinja_env.get_template("list_generic.html")
        return template.render(
            classname=classname,
            list=l,
            edit_screen="edit_screen_generic",
            primary_key="experimenter_id",
            help_page="ListGenericHelp",
        )

    # h4. edit_screen_generic
    @cherrypy.expose
    @logit.logstartstop
    def edit_screen_generic(self, classname, id=None):
        ctx = Ctx(experiment=experiment, role=role)
        if not self.permissions.is_superuser(ctx.db, ctx.username):
            raise cherrypy.HTTPError(401, "You are not authorized to access this resource")
        return self.tablesPOMS.edit_screen_generic(classname, id)

    # h4. update_generic
    @cherrypy.expose
    @logit.logstartstop
    def update_generic(self, classname, *args, **kwargs):
        ctx = Ctx(experiment=experiment, role=role)
        if not self.permissions.is_superuser(ctx.db, ctx.username):
            raise cherrypy.HTTPError(401, "You are not authorized to access this resource")
        return self.tablesPOMS.update_generic(ctx.db, classname, *args, **kwargs)

    # h4. edit_screen_for
    @cherrypy.expose
    @logit.logstartstop
    def edit_screen_for(self, classname, eclass, update_call, primkey, primval, valmap):
        ctx = Ctx(experiment=experiment, role=role)
        if not self.permissions.is_superuser(ctx.db, ctx.username):
            raise cherrypy.HTTPError(401, "You are not authorized to access this resource")
        screendata = self.tablesPOMS.edit_screen_for(ctx.db, classname, eclass, update_call, primkey, primval, valmap)
        template = self.jinja_env.get_template("edit_screen_for.html")
        return template.render(screendata=screendata, action="./" + update_call, classname=classname, help_page="GenericEditHelp")

    #######
    # JobPOMS

    # h4. force_locate_submission

    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def force_locate_submission(self, experiment, role, submission_id):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_view(ctx, "Submission", item_id=submission_id)
        return self.taskPOMS.force_locate_submission(ctx, submission_id)

    # h4. mark_failed_submissions
    @cherrypy.expose
    @logit.logstartstop
    def mark_failed_submissions(self):
        ctx = Ctx(experiment=experiment, role=role)
        return self.taskPOMS.mark_failed_submissions(ctx.db)

    # h4. running_submissions
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def running_submissions(self, campaign_id_list):
        ctx = Ctx()
        cl = list(map(int, campaign_id_list.split(",")))
        return self.taskPOMS.running_submissions(ctx, cl)

    # h4. update_submission
    @cherrypy.expose
    @logit.logstartstop
    def update_submission(self, submission_id, jobsub_job_id, pct_complete=None, status=None, project=None, redirect=None):
        ctx = Ctx()
        self.permissions.can_modify(ctx, "Submission", item_id=submission_id)
        res = self.taskPOMS.update_submission(
            ctx, submission_id, jobsub_job_id, status=status, project=project, pct_complete=pct_complete
        )
        if redirect:
            raise cherrypy.HTTPRedirect(ctx.headers_get("Referer"))
        return res

    # h3. File upload management for Analysis users
    #
    # h4. file_uploads
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def file_uploads(self, experiment, role, checkuser=None):
        ctx = Ctx(experiment=experiment, role=role)
        if role == "production":
            raise cherrypy.HTTPRedirect(self.path + "/index")
        self.permissions.can_view(ctx, "Experimenter", item_id=ctx.username)
        quota = ctx.config_get("base_uploads_quota", 10_485_760)
        file_stat_list, total, experimenters = self.filesPOMS.file_uploads(ctx, checkuser)
        template = self.jinja_env.get_template("file_uploads.html")
        return template.render(
            experiment=experiment,
            role=role,
            experimenters=experimenters,
            file_stat_list=file_stat_list,
            total=total,
            quota=quota,
            time=time,
            checkuser=checkuser,
        )

    # h4. file_uploads
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def file_uploads_json(self, experiment, role, checkuser=None):
        ctx = Ctx(experiment=experiment, role=role)
        quota = ctx.config_get("base_uploads_quota", 10_485_760)
        file_stat_list, total, experimenters = self.filesPOMS.file_uploads(ctx, checkuser)
        return {"file_stat_list": file_stat_list, "total": total, "quota": quota}

    # h4. upload_file
    @cherrypy.expose
    # @error_rewrite
    @logit.logstartstop
    def upload_file(self, experiment, role, filename, username=None, **kwargs):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_modify(ctx, "Experimenter", item_id=username)
        self.filesPOMS.upload_file(ctx, quota=ctx.config_get("base_uploads_quota"), filename=filename)
        raise cherrypy.HTTPRedirect("%s/file_uploads/%s/%s/%s" % (self.path, experiment, role, username))

    # h4. remove_uploaded_files
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def remove_uploaded_files(self, experiment, role, experimenter, filename, action, redirect=1):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_modify(ctx, "Experimenter", item_id=experimenter)
        res = self.filesPOMS.remove_uploaded_files(ctx.config_get("base_uploads_dir"), experiment, ctx.username, filename, action)
        if int(redirect) == 1:
            raise cherrypy.HTTPRedirect("%s/file_uploads/%s/%s/%s" % (self.path, experiment, role, experimenter))
        return res

    # h3. Job actions
    #
    # h4. kill_jobs

    @poms_method(
        p=[
            {"p": "can_do", "t": "Submission", "item_id": "submission_id"},
            {"p": "can_do", "t": "CampaignStage", "item_id": "campaign_stage_id"},
            {"p": "can_do", "t": "Campaign", "item_id": "campaign_id"},
        ],
        u=["output", "s", "campaign_stage_id", "submission_id", "job_id"],
        t="kill_jobs.html",
        confirm=True,
    )
    def kill_jobs(self, **kwargs):
        return self.jobsPOMS.kill_jobs(**kwargs)

    # h4. set_job_launches

    @cherrypy.expose
    @logit.logstartstop
    def set_job_launches(self, experiment, role, hold):
        ctx = Ctx(experiment=experiment, role=role)
        if not self.permissions.is_superuser(ctx.db, ctx.username):
            raise cherrypy.HTTPError(401, "You are not authorized to access this resource")
        self.taskPOMS.set_job_launches(ctx.db, experiment, role, ctx.username, hold)
        raise cherrypy.HTTPRedirect(self.path + "/index/%s/%s" % experiment, role)

    # h4. launch_queued_job

    @cherrypy.expose
    @logit.logstartstop
    def launch_queued_job(self):
        ctx = Ctx(experiment=experiment, role=role)
        return self.taskPOMS.launch_queued_job(ctx)

    # h4. launch_campaign
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def launch_campaign(
        self,
        experiment,
        role,
        campaign_id=None,
        dataset_override=None,
        parent_submission_id=None,
        parent_task_id=None,
        test_login_setup=None,
        launcher=None,
        test_launch=False,
        output_commands=None,
    ):
        ctx = Ctx(experiment=experiment, role=role)
        if ctx.username != "poms" or launcher == "":
            launch_user = ctx.username
        else:
            launch_user = launcher

        logit.log("calling launch_campaign with campaign_id='%s'" % campaign_id)
        self.permissions.can_do(ctx, "Campaign", item_id=campaign_id)

        vals = self.campaignsPOMS.launch_campaign(
            ctx, campaign_id, launch_user, test_launch=test_launch, output_commands=output_commands
        )

        logit.log("Got vals: %s" % repr(vals))

        if output_commands:
            cherrypy.response.headers["Content-Type"] = "text/plain"
            return vals

        lcmd, cs, campaign_stage_id, outdir, outfile = vals
        if lcmd == "":
            return "Launches held, job queued..."
        else:
            raise cherrypy.HTTPRedirect(
                "%s/list_launch_file/%s/%s?campaign_stage_id=%s&fname=%s"
                % (self.path, experiment, role, campaign_stage_id, os.path.basename(outfile))
            )

    # h4. test_split_type_editors

    # h4. launch_jobs
    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def launch_jobs(
        self,
        experiment,
        role,
        campaign_stage_id=None,
        dataset_override=None,
        parent_submission_id=None,
        parent_task_id=None,
        test_login_setup=None,
        launcher=None,
        test_launch=False,
        test_launch_template=None,
        campaign_id=None,
        test=None,
        output_commands=None,
    ):
        ctx = Ctx(experiment=experiment, role=role)
        if not campaign_stage_id and campaign_id:
            campaign_stage_id = campaign_id
        if not test_login_setup and test_launch_template:
            test_login_setup = test_launch_template
        if parent_task_id and not parent_submission_id:
            parent_submission_id = parent_task_id
        self.permissions.can_do(ctx, "CampaignStage", item_id=campaign_stage_id)
        if ctx.username != "poms" or not launcher:
            launch_user = ctx.username
        else:
            launch_user = launcher

        logit.log("calling launch_jobs with campaign_stage_id='%s'" % campaign_stage_id)

        vals = self.taskPOMS.launch_jobs(
            ctx,
            campaign_stage_id,
            launch_user,
            dataset_override=dataset_override,
            parent_submission_id=parent_submission_id,
            test_login_setup=test_login_setup,
            test_launch=test_launch,
            output_commands=output_commands,
        )
        logit.log("Got vals: %s" % repr(vals))

        if output_commands:
            cherrypy.response.headers["Content-Type"] = "text/plain"
            return vals

        lcmd, cs, campaign_stage_id, outdir, outfile = vals
        if lcmd == "":
            return outfile
        else:
            if test_login_setup:
                raise cherrypy.HTTPRedirect(
                    "%s/%s/%slist_launch_file?login_setup_id=%s&fname=%s"
                    % (self.path, experiment, role, test_login_setup, os.path.basename(outfile))
                )
            else:
                raise cherrypy.HTTPRedirect(
                    "%s/list_launch_file/%s/%s?campaign_stage_id=%s&fname=%s"
                    % (self.path, experiment, role, campaign_stage_id, os.path.basename(outfile))
                )

    # h4. launch_recovery_for

    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def launch_recovery_for(
        self, getconfig, experiment, role, submission_id=None, campaign_stage_id=None, recovery_type=None, launch=None
    ):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_do(ctx, "CampaignStage", item_id=campaign_stage_id)
        # we don't actually get the logfile, etc back from
        # launch_recovery_if_needed, so guess what it will be:

        s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).first()
        stime = datetime.datetime.now(utc)

        res = self.taskPOMS.launch_recovery_if_needed(
            ctx.db, ctx.sam, ctx.config_get, experiment, role, ctx.username, cherrypy.HTTPError, s, recovery_type
        )

        if res:
            new = (
                ctx.db.query(Submission)
                .filter(Submission.recovery_tasks_parent == submission_id, Submission.created >= stime)
                .first()
            )

            ds = new.created.astimezone(utc).strftime("%Y%m%d_%H%M%S")
            launcher_experimenter = new.experimenter_creator_obj
            outdir = "%s/private/logs/poms/launches/campaign_%s" % (os.environ["HOME"], campaign_stage_id)
            outfile = "%s/%s_%s_%s" % (outdir, ds, launcher_experimenter.username, new.submission_id)
            raise cherrypy.HTTPRedirect(
                "%s/list_launch_file/%s/%s?campaign_stage_id=%s&fname=%s"
                % (self.path, experiment, role, campaign_stage_id, os.path.basename(outfile))
            )
        else:
            return "No recovery needed, launch skipped."

    # h4. jobtype_list
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def jobtype_list(self, experiment, role, *args, **kwargs):
        ctx = Ctx(experiment=experiment, role=role)
        data = self.jobsPOMS.jobtype_list(ctx)
        return data

    # ----------------------
    ########################
    # TaskPOMS

    # h4. wrapup_tasks

    @cherrypy.expose
    @logit.logstartstop
    def wrapup_tasks(self):
        ctx = Ctx()
        if not self.permissions.is_superuser(ctx):
            raise cherrypy.HTTPError(401, "You are not authorized to access this resource")
        cherrypy.response.headers["Content-Type"] = "text/plain"
        return "\n".join(self.taskPOMS.wrapup_tasks(ctx))

    # h4. get_task_id_for

    @cherrypy.expose
    @logit.logstartstop
    def get_task_id_for(
        self,
        campaign,
        user=None,
        experiment=None,
        command_executed="",
        input_dataset="",
        parent_task_id=None,
        task_id=None,
        parent_submission_id=None,
        submission_id=None,
        campaign_id=None,
        test=None,
    ):

        return self.get_submission_id_for(
            campaign,
            user,
            experiment,
            command_executed,
            input_dataset,
            parent_task_id,
            task_id,
            parent_submission_id,
            submission_id,
            campaign_id,
            test,
        )

    # h4. get_submission_id_for
    @cherrypy.expose
    @logit.logstartstop
    def get_submission_id_for(
        self,
        campaign,
        user=None,
        experiment=None,
        command_executed="",
        input_dataset="",
        parent_task_id=None,
        task_id=None,
        parent_submission_id=None,
        submission_id=None,
        campaign_id=None,
        test=None,
    ):
        ctx = Ctx(experiment=experiment)
        if not campaign and campaign_id:
            campaign = campaign_id
        if task_id is not None and submission_id is None:
            submission_id = task_id
        if parent_task_id is not None and parent_submission_id is None:
            parent_submission_id = parent_task_id
        self.permissions.can_modify(ctx, "Campaign", item_id=campaign_id)
        submission_id = self.taskPOMS.get_task_id_for(
            ctx, campaign_id, command_executed, input_dataset, parent_submission_id, submission_id
        )
        return "Task=%d" % submission_id

    # h4. campaign_task_files

    @cherrypy.expose
    @error_rewrite
    @logit.logstartstop
    def campaign_task_files(self, experiment, role, campaign_stage_id=None, campaign_id=None, tmin=None, tmax=None, tdays=1):
        ctx = Ctx(experiment=experiment, role=role, tmin=tmin, tmax=tmax, tdays=tdays)
        self.permissions.can_view(ctx, "CampaignStage", item_id=campaign_stage_id)
        (cs, columns, datarows, tmins, tmaxs, prevlink, nextlink, tdays) = self.filesPOMS.campaign_task_files(
            ctx, campaign_stage_id, campaign_id
        )
        template = self.jinja_env.get_template("campaign_task_files.html")
        return template.render(
            name=cs.name if cs else "",
            experiment=experiment,
            role=role,
            CampaignStage=cs,
            columns=columns,
            datarows=datarows,
            tmin=tmins,
            tmax=tmaxs,
            prev=prevlink,
            next=nextlink,
            tdays=tdays,
            campaign_stage_id=campaign_stage_id,
            help_page="CampaignTaskFilesHelp",
        )

    # h4. show_dimension_files

    @cherrypy.expose
    @logit.logstartstop
    def show_dimension_files(self, experiment, role, dims):
        ctx = Ctx(experiment=experiment, role=role)
        flist = self.filesPOMS.show_dimension_files(ctx, dims)
        template = self.jinja_env.get_template("show_dimension_files.html")
        return template.render(
            flist=flist, dims=dims, statusmap=[], experiment=experiment, role=role, help_page="ShowDimensionFilesHelp"
        )

    # h4. link_tags
    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def link_tags(self, experiment, role, campaign_id=None, tag_name=None):
        ctx = Ctx(experiment=experiment, role=role)
        self.permissions.can_modify(ctx, "Campaign", item_id=campaign_id)
        return self.tagsPOMS.link_tags(ctx, campaign_id=campaign_id, tag_name=tag_name)

    # h4. delete_campaigns_tags

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def delete_campaigns_tags(self, experiment, role, campaign_id, tag_id, delete_unused_tag=False):
        self.permissions.can_modify(ctx, "Campaign", item_id=campaign_id)
        ctx = Ctx(experiment=experiment, role=role)
        return self.tagsPOMS.delete_campaigns_tags(ctx, campaign_id, tag_id, delete_unused_tag)

    # h4. search_tags
    @cherrypy.expose
    @logit.logstartstop
    def search_tags(self, q):
        ctx = Ctx(experiment=experiment, role=role)
        results = self.tagsPOMS.search_tags(ctx, tag_name=q)
        template = self.jinja_env.get_template("search_tags.html")
        return template.render(results=results, search_term=q, do_refresh=0, help_page="SearchTagsHelp")

    # h4. search_campaigns

    @cherrypy.expose
    @logit.logstartstop
    def search_campaigns(self, search_term):
        ctx = Ctx(experiment=experiment, role=role)
        results = self.tagsPOMS.search_campaigns(ctx.db, search_term)
        template = self.jinja_env.get_template("search_campaigns.html")
        return template.render(results=results, search_term=search_term, do_refresh=0, help_page="SearchTagsHelp")

    # h4. search_all_tags

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def search_all_tags(self, cl):
        ctx = Ctx(experiment=experiment, role=role)
        return self.tagsPOMS.search_all_tags(ctx, cl)

    # h4. auto_complete_tags_search

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @logit.logstartstop
    def auto_complete_tags_search(self, experiment, q):
        ctx = Ctx(experiment=experiment)
        cherrypy.response.headers["Content-Type"] = "application/json"
        return self.tagsPOMS.auto_complete_tags_search(ctx, q)

    # h4. split_type_javascript
    @cherrypy.expose
    @cherrypy.tools.response_headers(headers=[("Content-Type", "text/javascript")])
    def split_type_javascript(self):
        ctx = Ctx(experiment="samdev", role="analysis")
        data = self.campaignsPOMS.split_type_javascript(ctx)
        return data

    # h4. save_campaign
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def save_campaign(self, *args, **kwargs):
        ctx = Ctx()
        # Note: permissions check deferred to body because we
        #       have to unpack the json...
        data = self.campaignsPOMS.save_campaign(ctx, *args, **kwargs)
        return data

    # h4. get_jobtype_id
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_jobtype_id(self, experiment, role, name):
        ctx = Ctx(experiment=experiment, role=role)
        data = self.campaignsPOMS.get_jobtype_id(ctx, name)
        return data

    # h4. get_loginsetup_id
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_loginsetup_id(self, experiment, role, name):
        ctx = Ctx(experiment=experiment, role=role)
        data = self.campaignsPOMS.get_loginsetup_id(ctx, name)
        return data

    # h4. loginsetup_list
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def loginsetup_list(self, exp, role, name=None, full=None):
        ctx = Ctx(experiment=exp, role=role)
        if full:
            data = (
                ctx.db.query(LoginSetup.name, LoginSetup.launch_host, LoginSetup.launch_account, LoginSetup.launch_setup)
                .filter(LoginSetup.experiment == exp)
                .order_by(LoginSetup.name)
                .all()
            )
        else:
            data = ctx.db.query(LoginSetup.name).filter(LoginSetup.experiment == exp).order_by(LoginSetup.name).all()

        return [r._asdict() for r in data]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def ini_to_campaign(self, upload=None, **kwargs):
        ctx = Ctx(experiment=experiment, role=role)

        # Note: permission check deferred to save_campaign

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
            return {"status": "400 Bad Request", "message": "Bad file format"}

        cfg = {section: dict(p.items(section)) for section in p.sections()}
        pprint.pprint(cfg)

        # Now reformat into JSON suitable for save_campaign call
        campaign_d = {"stages": [], "dependencies": [], "misc": []}

        campaign_s = cfg.get("campaign")
        if campaign_s is None:
            return {"status": "400 Bad Request", "message": "No Campaign section in the file"}

        camp_name = campaign_s["name"]
        campaign_d["stages"].append(
            {"id": "campaign {}".format(camp_name), "label": camp_name, "clean": False, "form": cfg.get("campaign_defaults", {})}
        )
        # Process stages
        stage_names = [k for k in cfg if k.startswith("campaign_stage ")]
        if not stage_names:
            return {"status": "400 Bad Request", "message": "No Campaign Stage sections in the file"}

        for name in stage_names:
            sn = name.split(" ", 1)[1]
            campaign_d["stages"].append({"id": sn, "label": sn, "clean": False, "form": cfg[name]})
        # Process dependencies
        dep_names = [k for k in cfg if k.startswith("dependencies ")]
        for name in dep_names:
            section = cfg[name]
            sn = name.split(" ", 1)[1]
            from_ids = [k for k in section if k.startswith("campaign_stage_")]
            for from_id in from_ids:
                sfx = from_id.rsplit("_", 1)[1]
                sel = f"file_pattern_{sfx}"
                campaign_d["dependencies"].append(
                    {"id": f"{name}_{sfx}", "fromId": section[from_id], "toId": sn, "clean": False, "form": {sel: section[sel]}}
                )
        # Process JobTypes and LoginSetups
        misc_names = [k for k in cfg if k.startswith("job_type ") or k.startswith("login_setup ")]
        for name in misc_names:
            section = cfg[name]
            sn = name.split(" ", 1)[1]
            campaign_d["misc"].append({"id": name, "label": sn, "clean": False, "form": section})
        # Save the campaign
        data = self.campaignsPOMS.save_campaign(ctx.db, ctx.username, form=json.dumps(campaign_d), **kwargs)

        return data
