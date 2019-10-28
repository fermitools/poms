# h2. Module webservice.poms_service
#
# This module attaches all the webservice methods to
# the cherrypy instance, via the mount call in &l=webservice/service.py&
# the methods call out to one of the POMS logic modules, and
# generally either use jinja2 templates to render the results, or
# use the cherrypy @cherrypy.tools.json_out() decorator to
# yeild the result in JSON.
#
# Most of the calls use the @poms_method(...) decorator defined in
# &l=webservice/poms_method.py&
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
    MiscPOMS,
    StagesPOMS,
    SubmissionsPOMS,
    TablesPOMS,
    TagsPOMS,
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
        "error_page.407": "%s/%s" % (tdir, "launch_held.html"),
        "error_page.423": "%s/%s" % (tdir, "launch_held.html"),
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
        self.miscPOMS = MiscPOMS.MiscPOMS(self)
        self.stagesPOMS = StagesPOMS.StagesPOMS(self)
        self.submissionsPOMS = SubmissionsPOMS.SubmissionsPOMS(self)
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
        return {"version": self.version, "launches": self.submissionsPOMS.get_job_launches(kwargs["ctx"])}

    ####################
    # UtilsPOMS

    # h4. quick_search
    @poms_method()
    def quick_search(self, **kwargs):
        return self.utilsPOMS.quick_search(cherrypy.HTTPRedirect, **kwargs)

    # see &l=webservice/UtilsPOMS.py#quick_search&

    # h4. update_session_experiment
    @poms_method()
    def update_session_experiment(self, ctx, **kwargs):
        old_exp = ctx.experiment
        old_role = ctx.role
        self.utilsPOMS.update_session_experiment(ctx, **kwargs)

        raise cherrypy.HTTPRedirect(
            ctx.headers_get("Referer", "%s/index/%s/%s" % (self.path, old_exp, old_role))
            .replace(old_exp, ctx.experiment)
            .replace(old_role, ctx.role)
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
    def raw_tables(self, **kwargs):
        return {"tlist": list(self.tablesPOMS.admin_map.keys())}

    #
    # h4. experiment_list
    #
    # list of experiments we support for submission agent, etc to use.
    #
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}])
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
    def launch_template_edit(self, ctx, **kwargs):
        return self.login_setup_edit(ctx, **kwargs)

    # h4. login_setup_rm
    @poms_method(rtype="json")
    def login_setup_rm(self, **kwargs):
        # note: login_setup_edit checks for delete permission...
        kwargs["action"] = "delete"
        return self.miscPOMS.login_setup_edit(**kwargs)

    # see &l=webservice/MiscPOMS.py#login_setup_edit&

    # h4. login_setup_edit
    @poms_method(p=[{"p": "can_modify", "t": "LoginSetup", "name": "ae_launch_name"}], t="login_setup_edit.html")
    def login_setup_edit(self, ctx, **kwargs):
        res = {"data": self.miscPOMS.login_setup_edit(ctx, **kwargs)}
        if kwargs.get("test_template"):
            raise cherrypy.HTTPRedirect(
                "%s/launch_jobs/%s/%s?campaign_stage_id=None&test_login_setup=%s"
                % (self.path, ctx.experiment, ctx.role, data["login_setup_id"])  # FIXME: data is undefined!
            )
        return res

    # h4. campaign_deps_ini

    @poms_method(
        p=[{"p": "can_view", "t": "Campaign", "item_id": "camp_id"}, {"p": "can_view", "t": "Campaign", "item_id": "stage_id"}],
        rtype="ini",
    )
    def campaign_deps_ini(self, **kwargs):
        return self.campaignsPOMS.campaign_deps_ini(**kwargs)

    # see &l=webservice/CampaignsPOMS.py#campaign_deps_ini&

    # h4. campaign_deps

    @poms_method(
        p=[{"p": "can_view", "t": "Campaign", "name": "campaign_name"}], t="campaign_deps.html", help_page="CampaignDepsHelp"
    )
    def campaign_deps(self, **kwargs):
        return {"svgdata": self.campaignsPOMS.campaign_deps_svg(**kwargs)}

    # h4. job_type_rm
    @poms_method(rtype="json")
    def job_type_rm(self, **kwargs):
        # note: job_type_edit checks for delete permission
        return self.miscPOMS.job_type_edit(action="delete", **kwargs)

    # h4. modify_job_type_recoveries
    @poms_method(p=[{"p": "can_modify", "t": "JobType", "item_id": "job_type_id"}], rtype="json")
    def modify_job_type_recoveries(self, **kwargs):
        return self.miscPOMS.modify_job_type_recoveries(**kwargs)

    # see &l=webservice/CampaignsPOMS.py#job_type_edit&

    # h4. job_type_edit

    @poms_method(p=[{"p": "can_modify", "t": "LoginSetup", "name": "ae_launch_name"}], t="job_type_edit.html")
    def job_type_edit(self, ctx, **kwargs):
        res = {"data": self.miscPOMS.job_type_edit(ctx, **kwargs), "jquery_ui": False}
        if kwargs.get("test_template"):
            test_campaign = self.campaignsPOMS.make_test_campaign_for(
                ctx, kwargs.get("ae_campaign_definition_id"), kwargs.get("ae_definition_name")
            )
            raise cherrypy.HTTPRedirect(
                "%s/%s/%s/campaign_stage_edit?jump_to_campaign=%d&extra_edit_flag=launch_test_job"
                % (self.path, ctx.experiment, ctx.role, test_campaign)
            )
        return res

    # h4. make_test_campaign_for

    @poms_method(p=[{"p": "can_modify", "t": "JobType", "name": "campaign_def_name", "experiment": "experiment"}])
    def make_test_campaign_for(self, ctx, **kwargs):
        cid = self.campaignsPOMS.make_test_campaign_for(**kwargs)
        raise cherrypy.HTTPRedirect(
            "%s/campaign_stage_edit/%s/%s?campaign_stage_id=%d&extra_edit_flag=launch_test_job"
            % (self.path, ctx.experiment, ctx.role, cid)
        )

    # h4. get_campaign_id

    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Campaign", "name": "campaign_name"}])
    def get_campaign_id(self, **kwargs):
        return self.campaignsPOMS.get_campaign_id(**kwargs)

    # see &l=webservice/CampaignsPOMS.py#get_campaign_id&

    # h4. get_campaign_name

    @poms_method()
    def get_campaign_name(self, **kwargs):
        return self.campaignsPOMS.get_campaign_name(**kwargs)

    # see &l=webservice/CampaignsPOMS.py#get_campaign_name&

    # h4. get_campaign_stage_id

    @poms_method(
        rtype="json", p=[{"p": "can_view", "t": "CampaignStage", "name": "campaign_stage_name", "campaign_name": "campaign_name"}]
    )
    def get_campaign_stage_id(self, **kwargs):
        return self.stagesPOMS.get_campaign_stage_id(**kwargs)

    # see &l=webservice/StagesPOMS.py#get_campaign_stage_id&

    # h4. get_campaign_stage_name

    @poms_method(rtype="json", p=[{"p": "can_view", "t": "CampaignStage", "item_id": "campaign_stage_id"}])
    def get_campaign_stage_name(self, **kwargs):
        return self.stagesPOMS.get_campaign_stage_name(**kwargs)

    # see &l=webservice/StagesPOMS.py#get_campaign_stage_name&

    # h4. update_stage_param_overrides

    @poms_method(
        p=[
            {"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage"},
            {"p": "can_modify", "t": "CampaignStage", "name": "campaign_stage"},
        ]
    )
    def update_stage_param_overrides(self, **kwargs):
        return self.stagesPOMS.update_stage_param_overrides(**kwargs)

    # see &l=webservice/StagesPOMS.py#update_stage_param_overrides&

    # h4. campaign_add_name

    @poms_method(p=[{"p": "can_modify", "t": "Experiment", "item_id": "experiment"}])
    def campaign_add_name(self, **kwargs):
        return self.campaignsPOMS.campaign_add_name(**kwargs)

    # see &l=webservice/CampaignsPOMS.py#campaign_add_name&

    # h4. campaign_stage_edit
    @poms_method(
        p=[{"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        help_page="POMS_User_Documentation",
        t="campaign_stage_edit.html",
    )
    def campaign_stage_edit(self, ctx, **kwargs):
        data = self.stagesPOMS.campaign_stage_edit(ctx, **kwargs)
        if kwargs.get("pcl_call", "0") == "1" and data["message"]:
            raise cherrypy.HTTPError(400, data["message"])

        if kwargs.get("launch_test_job", None) and kwargs.get("ae_campaign_id", None):
            raise cherrypy.HTTPRedirect(
                "%s/%s/%s/launch_jobs?campaign_stage_id=%s" % (self.path, ctx.experiment, ctx.role, kwargs.get("ae_campaign_id"))
            )

        return {"data": data, "jquery_ui": False}

    # h4. update_campaign_stage
    @poms_method(
        p=[
            {"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage"},
            {"p": "can_modify", "t": "CampaignStage", "name": "campaign_stage"},
        ]
    )
    def update_campaign_stage(self, **kwargs):
        return self.stagesPOMS.update_campaign_stage(**kwargs)

    # h4. gui_wf_edit
    @poms_method(
        p=[{"p": "can_modify", "t": "Campaign", "name": "campaign", "experiment": "experiment"}],
        t="gui_wf_edit.html",
        help_page="GUI_Workflow_Editor_User_Guide",
        need_er=True,
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

    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}])
    def campaign_list_json(self, **kwargs):
        return self.campaignsPOMS.campaign_list(kwargs["ctx"])

    # see &l=webservice/CampaignsPOMS.py#campaign_list&

    # h4. campaign_stage_edit_query

    @poms_method(
        rtype=json,
        p=[
            {"p": "can_view", "t": "LoginSetup", "item_id": "ae_launch_id"},
            {"p": "can_view", "t": "JobType", "item_id": "ae_campaign_definition_id"},
        ],
    )
    def campaign_stage_edit_query(self, **kwargs):
        return self.stagesPOMS.campaign_stage_edit_query(**kwargs)

    # see &l=webservice/StagesPOMS.py#campaign_stage_edit_query&

    # h4. show_campaigns

    @poms_method(
        p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}],
        t="show_campaigns.html",
        u=["tl", "last_activity", "msg", "data"],
        need_er=True,
    )
    def show_campaigns(self, **kwargs):
        return self.campaignsPOMS.show_campaigns(**kwargs)

    # see &l=webservice/CampaignsPOMS.py#show_campaigns&

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
        ],
        need_er=True,
    )
    def show_campaign_stages(self, **kwargs):
        if kwargs.get("campaign_ids", None) is None:
            template = "show_campaign_stages.html"
        else:
            template = "show_campaign_stages_stats.html"
        return self.stagesPOMS.show_campaign_stages(**kwargs) + (template, kwargs["ctx"].experiment, "")

    # see &l=webservice/StagesPOMS.py#show_campaign_stages&

    # h4. reset_campaign_split

    @poms_method(
        p=[{"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage"}],
        redirect="%(poms_path)s/campaign_stage_info/%(experiment)s/%(role)s?campaign_stage_id=%(campaign_stage_id)s",
        rtype="redirect",
    )
    def reset_campaign_split(self, **kwargs):
        return self.stagesPOMS.reset_campaign_split(**kwargs)

    # see &l=webservice/StagesPOMS.py#reset_campaign_split&

    # h4. campaign_stage_datasets
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}])
    def campaign_stage_datasets(self, **kwargs):
        return self.submissionsPOMS.campaign_stage_datasets(kwargs["ctx"])

    # see &l=webservice/SubmissionsPOMS.py#campaign_stage_datasets&

    # h4. submission_details
    @poms_method(
        p=[{"p": "can_view", "t": "Submission", "item_id": "submission_id"}],
        u=["submission", "history", "dataset", "recoverymap", "statusmap", "ds", "submission_log_format"],
        t="submission_details.html",
        help_page="SubmissionDetailsHelp",
    )
    def submission_details(self, **kwargs):
        return self.submissionsPOMS.submission_details(**kwargs)

    # see &l=webservice/SubmissionsPOMS.py#submission_details&

    # h4. campaign_stage_info

    @poms_method(
        p=[{"p": "can_view", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        u=[
            "Campaign_info",
            "time_range_string",
            "tmins",
            "tmaxs",
            "tdays",
            "Campaign_definition_info",
            "login_setup_info",
            "campaigns",
            "launched_campaigns",
            "dimlist",
            "CampaignStage",
            "counts_keys",
            "counts",
            "launch_flist",
            "kibana_link",
            "dep_svg",
            "last_activity",
            "recent_submissions",
        ],
        help_page="POMS_UserDocumentation",
        t="campaign_stage_info.html",
    )
    def campaign_stage_info(self, **kwargs):
        return self.stagesPOMS.campaign_stage_info(**kwargs)

    # see &l=webservice/StagesPOMS.py#campaign_stage_info&

    #   h4. campaign_stage_submissions
    @poms_method(
        p=[
            {"p": "can_view", "t": "CampaignStage", "item_id": "campaign_stage_id"},
            {"p": "can_view", "t": "Campaign", "item_id": "campaign_id"},
        ],
        t="campaign_stage_submissions.html",
    )
    def campaign_stage_submissions(self, **kwargs):
        return {"data": self.stagesPOMS.campaign_stage_submissions(**kwargs)}

    #   h4. session_status_history
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Submission", "item_id": "submission_id"}])
    def session_status_history(self, **kwargs):
        return self.submissionsPOMS.session_status_history(**kwargs)

    # see &l=webservice/SubmissionsPOMS.py#session_status_history&

    #   h4. abort_launch
    @poms_method(p=[{"p": "can_do", "t": "Submission", "item_id": "submission_id"}], rtype="text")
    def abort_launch(self, **kwargs):
        return self.submissionsPOMS.abort_launch(**kwargs)

    # see &l=webservice/SubmissionsPOMS.py#abort_launch&

    # h4. list_launch_file
    @poms_method(
        p=[{"p": "can_view", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        u=["lines", "do_refresh", "campaign_name", "stage_name"],
        t="launch_jobs.html",
        help_page="LaunchedJobsHelp",
    )
    def list_launch_file(self, **kwargs):
        return self.filesPOMS.list_launch_file(**kwargs)

    # see &l=webservice/FilesPOMS.py#list_launch_file&

    # h4. schedule_launch

    @poms_method(
        p=[{"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        t="schedule_launch.html",
        u=["cs", "job", "launch_flist"],
        help_page="ScheduleLaunchHelp",
    )
    def schedule_launch(self, **kwargs):
        return self.stagesPOMS.schedule_launch(**kwargs)

    # see &l=webservice/StagesPOMS.py#schedule_launch&

    # h4. update_launch_schedule

    @poms_method(
        p=[{"p": "can_modify", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        rtype="redirect",
        redirect="%(poms_path)s/schedule_launch/%(experiment)s/%(role)s?campaign_stage_id=%(campaign_stage_id)s",
    )
    def update_launch_schedule(self, **kwargs):
        self.stagesPOMS.update_launch_schedule(**kwargs)

    # h4. mark_campaign_active

    @poms_method(p=[{"p": "can_modify", "t": "Campaign", "item_id": "campaign_id"}])
    def mark_campaign_active(self, **kwargs):
        self.campaignsPOMS.mark_campaign_active(**kwargs)

    # h4. mark_campaign_hold
    @poms_method(rtype="redirect", redirect="%(poms_path)s/show_campaign_stages/%(experiment)s/%(role)s")
    def mark_campaign_hold(self, **kwargs):
        kwargs["campaign_stage_ids"] = [int(x) for x in kwargs["ids2HR"].split(",")]
        logit.log("ids2HR is %s" % kwargs["ids2HR"])
        logit.log("campaign_stage_ids is %s" % kwargs["campaign_stage_ids"])
        del kwargs["ids2HR"]
        for cid in kwargs["campaign_stage_ids"]:
            self.permissions.can_modify(kwargs["ctx"], t="CampaignStage", item_id=cid)
        return self.stagesPOMS.mark_campaign_hold(**kwargs)

    # see &l=webservice/StagesPOMS.py#mark_campaign_hold&

    # h4. make_stale_campaigns_inactive

    @poms_method(p=[{"p": "is_superuser"}])
    def make_stale_campaigns_inactive(self, **kwargs):
        res = self.campaignsPOMS.make_stale_campaigns_inactive(kwargs["ctx"])
        return "Marked inactive stale: " + ",".join(res)

    # h4. list_generic

    @poms_method(p=[{"p": "is_superuser"}], t="list_generic.html", help_page="ListGenericHelp")
    def list_generic(self, **kwargs):
        return {
            "list": self.tablesPOMS.list_generic(**kwargs),
            "edit_screen": "edit_screen_generic",
            "primary_key": "experimenter_id",
        }

    @poms_method(p=[{"p": "is_superuser"}])
    def edit_screen_generic(self, **kwargs):
        return self.tablesPOMS.edit_screen_generic(**kwargs)

    # see &l=webservice/TablesPOMS.py#edit_screen_generic&

    # h4. update_generic
    @poms_method(p=[{"p": "is_superuser"}], rtype="html")
    def update_generic(self, **kwargs):
        return self.tablesPOMS.update_generic(**kwargs)

    # see &l=webservice/TablesPOMS.py#update_generic&

    # h4. edit_screen_for
    # this is a little odd, it gets called sideways
    # (see edit_screen__generic in TablesPOMS...)
    @poms_method(p=[{"p": "is_superuser"}], t="edit_screen_for.html", help_page="GenericEditHelp", call_args=True)
    def edit_screen_for(self, *args):
        return {"screendata": self.tablesPOMS.edit_screen_for(*args), "action": "./" + args[3], "classname": args[1]}

    #######
    # JobPOMS

    # h4. force_locate_submission

    @poms_method(p=[{"p": "can_view", "t": "Submission", "item_id": "submission_id"}])
    def force_locate_submission(self, **kwargs):
        return self.submissionsPOMS.force_locate_submission(kwargs["ctx"], kwargs["submission_id"])

    # see &l=webservice/SubmissionsPOMS.py#force_locate_submission&

    # h4. mark_failed_submissions
    @poms_method(p=[{"p": "is_superuser"}])
    def mark_failed_submissions(self, **kwargs):
        return self.submissionsPOMS.mark_failed_submissions(kwargs["ctx"])

    # see &l=webservice/SubmissionsPOMS.py#mark_failed_submissions&

    # h4. running_submissions
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}])
    def running_submissions(self, **kwargs):
        cl = list(map(int, kwargs["campaign_id_list"].split(",")))
        return self.submissionsPOMS.running_submissions(kwargs["ctx"], cl)

    # see &l=webservice/SubmissionsPOMS.py#running_submissions&

    # h4. update_submission
    @poms_method(p=[{"p": "can_do", "t": "Submission", "item_id": "submission_id"}])
    def update_submission(self, **kwargs):
        res = self.submissionsPOMS.update_submission(**kwargs)
        if kwargs.get("redirect", None):
            raise cherrypy.HTTPRedirect(kwargs["ctx"].headers_get("Referer"))
        return res

    # h3. File upload management for Analysis users
    #
    # h4. file_uploads
    @poms_method(
        p=[{"p": "can_view", "t": "Experimenter", "item_id": "username"}],
        u=["file_stat_list", "total", "experimenters", "quota"],
        t="file_uploads.html",
    )
    def file_uploads(self, **kwargs):
        if kwargs["ctx"].role == "production":
            raise cherrypy.HTTPRedirect(self.path + "/index")
        return self.filesPOMS.file_uploads(kwargs["ctx"], kwargs.get("checkuser", None))

    # see &l=webservice/FilesPOMS.py#file_uploads&

    # h4. file_uploads_json
    @poms_method(
        rtype="json",
        p=[{"p": "can_view", "t": "Experimenter", "item_id": "username"}],
        u=["file_stat_list", "total", "experimenters", "quota"],
        t="file_uploads.html",
    )
    def file_uploads_json(self, ctx, checkuser=None):
        return self.filesPOMS.file_uploads(ctx, checkuser)

    # see &l=webservice/FilesPOMS.py#file_uploads&

    # h4. upload_file
    @poms_method(
        p=[{"p": "can_modify", "t": "Experimenter", "item_id": "username"}],
        rtype="redirect",
        redirect="%(poms_path)s/file_uploads/%(experiment)s/%(role)s/%(username)s",
        need_er=True,
    )
    def upload_file(self, ctx, **kwargs):
        return self.filesPOMS.upload_file(
            # see &l=webservice/FilesPOMS.py#upload_file&
            ctx,
            filename=kwargs["filename"],
        )

    @poms_method(p=[{"p": "can_modify", "t": "Experimenter", "item_id": "username"}], rtype="text/plain")
    def download_file(self, **kwargs):
        return self.filesPOMS.download_file(ctx=kwargs["ctx"], filename=kwargs["filename"])

    # h4. remove_uploaded_files
    @poms_method(
        p=[{"p": "can_modify", "t": "Experimenter", "item_id": "username"}],
        rtype="redirect",
        redirect="%(poms_path)s/file_uploads/%(experiment)s/%(role)s/%(username)s",
    )
    def remove_uploaded_files(self, **kwargs):
        return self.filesPOMS.remove_uploaded_files(**kwargs)

    # see &l=webservice/FilesPOMS.py#remove_uploaded_files&

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

    # see &l=webservice/JobsPOMS.py#kill_jobs&

    # h4. set_job_launches

    @poms_method(p=[{"p", "is_superuser"}], rtype="redirect", redirect="%(poms_path)s/index/%(experiment)s/%(role)s")
    def set_job_launches(self, ctx, **kwargs):
        return self.submissionsPOMS.set_job_launches(ctx, hold)  # FIXME: 'hold' is undefined

    # see &l=webservice/SubmissionsPOMS.py#set_job_launches&

    # h4. launch_queued_job

    @poms_method(p=[{"p", "is_superuser"}])
    def launch_queued_job(self, ctx, **kwargs):
        return self.submissionsPOMS.launch_queued_job(ctx)

    # see &l=webservice/SubmissionsPOMS.py#launch_queued_job&

    # h4. launch_campaign
    @poms_method(
        p=[{"p": "can_do", "t": "Campaign", "item_id": "campaign_id"}],
        u=["lcmd", "cs", "campaign_stage_id", "outdir", "outfile"],
        rtype="redirect",
        redirect="%(poms_path)s/list_launch_file/%(experiment)s/%(role)s?campaign_stage_id=%(campaign_stage_id)s&fname=%(outfile)s",
    )
    def launch_campaign(self, ctx, **kwargs):
        if ctx.username != "poms" or kwargs.get("launcher", "") == "":
            launch_user = ctx.username
        else:
            launch_user = kwargs.get("launcher", "")

        return self.campaignsPOMS.launch_campaign(ctx, **kwargs)

    # see &l=webservice/CampaignsPOMS.py#launch_campaign&

    # h4. test_split_type_editors

    @poms_method(
        p=[{"p": "can_do", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        u=["lcmd", "cs", "campaign_stage_id", "outdir", "outfile"],
        rtype="redirect",
        redirect="%(poms_path)s/list_launch_file/%(experiment)s/%(role)s?campaign_stage_id=%(campaign_stage_id)s&fname=%(outfile)s",
    )
    def launch_jobs(self, **kwargs):
        return self.submissionsPOMS.launch_jobs(**kwargs)

    # see &l=webservice/SubmissionsPOMS.py#launch_jobs&

    @poms_method(p=[{"p": "can_do", "t": "CampaignStage", "item_id": "campaign_stage_id"}])
    def launch_jobs_commands(self, **kwargs):
        return self.submissionsPOMS.launch_jobs(**kwargs)[0]

    # see &l=webservice/SubmissionsPOMS.py#launch_jobs&

    @poms_method(
        p=[{"p": "can_do", "t": "LoginSetup", "item_id": "test_login_setup"}],
        u=["lcmd", "cs", "campaign_stage_id", "outdir", "outfile"],
        rtype="redirect",
        redirect="%(poms_path)s/list_launch_file/%(experiment)s/%(role)s?login_setup_id=%(test_login_setup)s&fname=%(outfile)s",
    )
    def launch_login_setup(self, **kwargs):
        return self.submissionsPOMS.launch_jobs(**kwargs)

    # see &l=webservice/SubmissionsPOMS.py#launch_jobs&

    # h4. launch_recovery_for
    @poms_method(
        p=[{"p": "can_do", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        u=["outdir", "outfile", "outfullpath"],
        rtype="redirect",
        redirect="%(poms_path)s/list_launch_file/%(experiment)s/%(role)s?campaign_stage_id=%(campaign_stage_id)s&fname=%(outfile)s",
    )
    def launch_recovery_for(self, **kwargs):
        return self.submissionsPOMS.launch_recovery_for(**kwargs)

    # see &l=webservice/SubmissionsPOMS.py#launch_recovery_for&

    # h4. jobtype_list
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}])
    def jobtype_list(self, **kwargs):
        data = self.jobsPOMS.jobtype_list(kwargs["ctx"])
        return data

    # ----------------------
    ########################
    # SubmissionsPOMS

    # h4. wrapup_tasks

    @poms_method(p=[{"p": "is_superuser"}], rtype="text")
    def wrapup_tasks(self, **kwargs):
        return "\n".join(self.submissionsPOMS.wrapup_tasks(kwargs["ctx"]))

    # h4. get_task_id_for

    @cherrypy.expose
    @logit.logstartstop
    def get_task_id_for(self, **kwargs):
        return self.get_submission_id_for(**kwargs)

    # h4. get_submission_id_for
    @poms_method(p=[{"p": "can_do", "t": "CampaignStage", "item_id": "campaign_stage_id"}])
    def get_submission_id_for(self, **kwargs):
        return "Task=%d" % self.submissionsPOMS.get_task_id_for(**kwargs)

    # h4. campaign_task_files
    @poms_method(
        p=[{"p": "can_view", "t": "CampaignStage", "item_id": "campaign_stage_id"}],
        u=["cs", "columns", "datarows", "tmins", "tmaxs", "prev", "next", "tdays"],
        t="campaign_task_files.html",
        help_page="CampaignTaskFilesHelp",
    )
    def campaign_task_files(self, **kwargs):
        return self.filesPOMS.campaign_task_files(**kwargs)

    # see &l=webservice/FilesPOMS.py#campaign_task_files&

    # h4. show_dimension_files

    @poms_method(t="show_dimension_files.html")
    def show_dimension_files(self, **kwargs):
        return {"flist": self.filesPOMS.show_dimension_files(kwargs["ctx"], kwargs["dims"])}

    # h4. link_tags
    @poms_method(rtype="json", p=[{"p": "can_modify", "t": "Campaign", "item_id": "campaign_id"}])
    def link_tags(self, **kwargs):
        return self.tagsPOMS.link_tags(**kwargs)

    # see &l=webservice/TagsPOMS.py#link_tags&

    # h4. delete_campaigns_tags

    @poms_method(rtype="json", p=[{"p": "can_modify", "t": "Campaign", "item_id": "campaign_id"}])
    def delete_campaigns_tags(self, **kwargs):
        return self.tagsPOMS.delete_campaigns_tags(**kwargs)

    # see &l=webservice/TagsPOMS.py#delete_campaigns_tags&

    # h4. search_tags
    @poms_method(t="search_tags.html", help_page="SearchTagsHelp")
    def search_tags(self, **kwargs):
        return {"results": self.tagsPOMS.search_tags(kwargs["ctx"], tag_name=kwargs["q"])}

    # h4. search_campaigns

    @poms_method(t="search_campaigns.html", help_page="SearchTagsHelp")
    def search_campaigns(self, **kwargs):
        return {"results": self.tagsPOMS.search_campaigns(kwargs["ctx"], kwargs["search_term"])}

    # h4. search_all_tags

    @poms_method(rtype="json")
    def search_all_tags(self, **kwargs):
        return self.tagsPOMS.search_all_tags(ctx=kwargs["ctx"], cl=kwargs["cl"])

    # see &l=webservice/TagsPOMS.py#search_all_tags&

    # h4. auto_complete_tags_search

    @poms_method(rtype="json")
    def auto_complete_tags_search(self, **kwargs):
        return self.tagsPOMS.auto_complete_tags_search(kwargs["ctx"], kwargs["q"])

    # see &l=webservice/TagsPOMS.py#auto_complete_tags_search&

    # h4. split_type_javascript
    @poms_method(rtype="rawjavascript")
    def split_type_javascript(self, **kwargs):
        return self.miscPOMS.split_type_javascript(kwargs["ctx"])

    # see &l=webservice/MiscPOMS.py#split_type_javascript&

    # h4. save_campaign
    #
    # note: this does the permissions check down in the code, because
    # we have to parse the post data to get the name, etc.
    #
    @poms_method(rtype="json")
    def save_campaign(self, *args, **kwargs):
        return self.campaignsPOMS.save_campaign(*args, **kwargs)

    # see &l=webservice/CampaignsPOMS.py#save_campaign&

    # h4. get_jobtype_id
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "JobType", "name": "name"}])
    def get_jobtype_id(self, ctx, **kwargs):
        return self.miscPOMS.get_jobtype_id(ctx, kwargs["name"])

    # see &l=webservice/MiscPOMS.py#get_jobtype_id&

    # h4. get_loginsetup_id
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "LoginSetup", "name": "name"}])
    def get_loginsetup_id(self, ctx, **kwargs):
        return self.miscPOMS.get_loginsetup_id(ctx, kwargs["name"])

    # see &l=webservice/MiscPOMS.py#get_loginsetup_id&

    # h4. loginsetup_list
    @poms_method(rtype="json", p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}])
    def loginsetup_list(self, **kwargs):
        if kwargs.get("full", None):
            data = (
                kwargs["ctx"]
                .db.query(LoginSetup.name, LoginSetup.launch_host, LoginSetup.launch_account, LoginSetup.launch_setup)
                .filter(LoginSetup.experiment == kwargs["ctx"].experiment)
                .order_by(LoginSetup.name)
                .all()
            )
        else:
            data = (
                kwargs["ctx"]
                .db.query(LoginSetup.name)
                .filter(LoginSetup.experiment == kwargs["ctx"].experiment)
                .order_by(LoginSetup.name)
                .all()
            )

        return [r._asdict() for r in data]

    # h4. ini_to_campaign
    @poms_method(rtype="json")
    def ini_to_campaign(self, ctx, upload=None, **kwargs):
        # Note: permission check deferred to save_campaign

        # import pprint
        if not upload.file:
            return "Pick the file first!"

        data = upload.file.read().decode("utf-8")

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
        data = self.campaignsPOMS.save_campaign(ctx, form=json.dumps(campaign_d), **kwargs)

        return data

    @poms_method(
        t="held_launches.html", help_page="HeldLaunchesHelp", p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}]
    )
    def held_launches(self, **kwargs):
        return self.miscPOMS.held_launches(**kwargs)

    @poms_method(
        redirect="%(poms_path)s/held_launches/%(experiment)s/%(role)s",
        rtype="redirect",
        p=[{"p": "can_view", "t": "Experiment", "item_id": "experiment"}],
    )
    def held_launches_remove(self, **kwargs):
        return self.miscPOMS.held_launches_remove(**kwargs)
