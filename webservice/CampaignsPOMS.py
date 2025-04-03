#!/usr/bin/env python
"""
This module contain the methods that allow to create campaign_stages, definitions and templates.
List of methods:

Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in
poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: April 28th, 2017. (changes for the POMS_client)
"""
from __future__ import division
import glob
import importlib
import json
import os
import subprocess
import time
import traceback
from collections import OrderedDict, deque, defaultdict
from datetime import datetime, timedelta
import re
from unicodedata import name
import uuid

import cherrypy
from crontab import CronTab
from sqlalchemy import and_, distinct, func, or_, text, Integer
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import joinedload, attributes, aliased

from . import logit
from .poms_model import (
    Campaign,
    CampaignStage,
    CampaignsTag,
    JobType,
    CampaignDependency,
    CampaignRecovery,
    CampaignStageSnapshot,
    Experiment,
    Experimenter,
    ExperimentersWatching,
    LoginSetup,
    RecoveryType,
    Submission,
    SubmissionHistory,
    SubmissionStatus,
)
from .utc import utc
from .SAMSpecifics import sam_specifics


class CampaignsPOMS:
    """
       Business logic for CampaignStage related items
    """

    # h3. __init__
    def __init__(self, ps):
        """
            initialize ourself with a reference back to the overall poms_service
        """
        self.poms_service = ps

    # h3. get_campaign_id
    def get_campaign_id(self, ctx, campaign_name):
        """
            return the campaign id for a campaign name in our experiment
        """
        camp = ctx.db.query(Campaign).filter(Campaign.name == campaign_name, Campaign.experiment == ctx.experiment).scalar()
        return camp.campaign_id if camp else None

    # h3. get_campaign_name
    def get_campaign_name(self, ctx, campaign_id):
        """
            return the campaign name for a campaign id in our experiment
        """
        camp = ctx.db.query(Campaign).filter(Campaign.campaign_id == campaign_id, Campaign.experiment == ctx.experiment).scalar()
        return camp.name if camp else None

    # h3. campaign_add_name
    def campaign_add_name(self, ctx, **kwargs):
        """
            Add a new campaign name.
        """
        experimenter = ctx.get_experimenter()
        data = {}
        name = kwargs.get("campaign_name")
        data["message"] = "ok"
        try:
            camp = Campaign(name=name, experiment=ctx.experiment, creator=experimenter.experimenter_id, creator_role=ctx.role)
            ctx.db.add(camp)
            ctx.db.commit()
            c_s = CampaignStage(
                name="stage0",
                experiment=ctx.experiment,
                campaign_id=camp.campaign_id,
                # active=False,
                #
                completion_pct="95",
                completion_type="complete",
                cs_split_type="None",
                default_clear_cronjob = True,
                dataset="from_parent",
                job_type_id=(
                    ctx.db.query(JobType.job_type_id)
                    .filter(JobType.name == "generic", JobType.experiment == ctx.experiment)
                    .scalar()
                    or ctx.db.query(JobType.job_type_id)
                    .filter(JobType.name == "generic", JobType.experiment == "samdev")
                    .scalar()
                ),
                login_setup_id=(
                    ctx.db.query(LoginSetup.login_setup_id)
                    .filter(LoginSetup.name == "generic", LoginSetup.experiment == ctx.experiment)
                    .scalar()
                    or ctx.db.query(LoginSetup.login_setup_id)
                    .filter(LoginSetup.name == "generic", LoginSetup.experiment == "samdev")
                    .scalar()
                ),
                param_overrides=[],
                software_version="v1_0",
                test_param_overrides=[],
                vo_role="Production",
                #
                creator=experimenter.experimenter_id,
                creator_role=ctx.role,
                created=datetime.now(utc),
                campaign_stage_type="regular",
            )
            ctx.db.add(c_s)
        except IntegrityError as exc:
            data["message"] = "Integrity error: " "you are most likely using a name which " "already exists in database."
            logit.log(" ".join(exc.args))
            ctx.db.rollback()
        except SQLAlchemyError as exc:
            data["message"] = "SQLAlchemyError: " "Please report this to the administrator. " "Message: %s" % " ".join(exc.args)
            logit.log(" ".join(exc.args))
            ctx.db.rollback()
        else:
            ctx.db.commit()
        return json.dumps(data)

    # h3. campaign_list
    def campaign_list(self, ctx):
        """
            Return list of all campaign_stage_id s and names. --
            This is actually for Landscape to use.
        """
        data = ctx.db.query(CampaignStage.campaign_stage_id, CampaignStage.name, CampaignStage.experiment).all()
        return [r._asdict() for r in data]

    def get_leading_stages(self, ctx, campaign_id):
        # subquery to count dependencies
        q = (
            text(
                "select campaign_stage_id from campaign_stages "
                " where campaign_id = :campaign_id "
                "   and 0 = (select count(campaign_dep_id) "
                "  from campaign_dependencies "
                " where provides_campaign_stage_id = campaign_stage_id)"
            )
            .bindparams(campaign_id=campaign_id)
            .columns(campaign_stage_id=Integer)
        )

        stages = ctx.db.execute(q).fetchall()
        stages = [x[0] for x in stages]
        return stages

    def campaign_overview(self, ctx, campaign_id):

        logit.log(logit.INFO, "entering campaign_overview: %s" % campaign_id)

        stages = self.get_leading_stages(ctx, campaign_id)

        logit.log(logit.INFO, "leading stages: %s" % repr(stages))

        if len(stages) > 0:
            lead = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == stages[0]).first()
        else:
            lead = None

        exp = str(ctx.db.query(Campaign.experiment).filter(Campaign.campaign_id == campaign_id).first())

        #
        # for now we're skipping multiparam datasets
        # and stagedfiles ones; they are expandable though...
        #
        if lead:
            dslist = (
                ctx.db.query(distinct(CampaignStageSnapshot.dataset))
                .filter(CampaignStageSnapshot.campaign_stage_id == lead.campaign_stage_id)
                .filter(CampaignStageSnapshot.cs_split_type != "multiparam")
                .all()
            )

            dslist = [x[0] for x in dslist if x[0]]
        else:
            dslist = []

        # we start total very small so we don't divide by zero later if
        # there arent any..
       
        sp_list = (
            ctx.db.query(Campaign.campaign_id, CampaignStage, Submission)
            .filter(CampaignStage.campaign_id == campaign_id, Campaign.campaign_id == campaign_id, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
            .order_by(CampaignStage.campaign_stage_id, Submission.submission_id)
            .all()
        )
        sp_list = [[x[1].name, x[2]] for x in sp_list if x[2]]
        
        total = 0.0
        for ds in dslist:
            count = ctx.sam.count_files(exp, "defname:%s" % ds)
            if count > 0:
                total += count
        if total == 0:
            subs = [x[1] for x in sp_list if x[1]]
            for sub in subs:
                if sub.files_generated:
                    total += sub.files_generated
        logit.log("campaign_overview: total: %d" % total)
        """
        
        listfiles = "../../../show_dimension_files/%s/%s?dims=%%s" % (ctx.experiment, ctx.role)
        (
            summary_list,
            some_kids_decl_needed,
            some_kids_needed,
            base_dim_list,
            output_files,
            output_list,
            all_kids_decl_needed,
            some_kids_list,
            some_kids_decl_list,
            all_kids_decl_list,
        ) = sam_specifics(ctx).get_file_stats_for_submissions(subs, ctx.experiment)
        """
        counts = (
            ctx.db.query(
                CampaignStage.campaign_stage_id, func.sum(Submission.files_consumed), func.sum(Submission.files_generated)
            )
            .filter(CampaignStage.campaign_id == campaign_id, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
            .group_by(CampaignStage.campaign_stage_id)
            .all()
        )
        consumed_map = {}
        generated_map = {}
        """
        for i in range(0, len(subs)):
            if subs[i].campaign_stage_id not in consumed_map.items():
                consumed_map[subs[i].campaign_stage_id] = 0
            if subs[i].campaign_stage_id not in generated_map.items():
                generated_map[subs[i].campaign_stage_id] = 0
            consumed_map[subs[i].campaign_stage_id] += summary_list[i].get("tot_consumed", 0)
            generated_map[subs[i].campaign_stage_id] += output_list[i]
            total += output_list[i]
        """
        for r in counts:
            consumed_map[r[0]] = r[1]
            generated_map[r[0]] = r[2]

       
        logit.log("campaign_overview: consumed_map: %s" % repr(consumed_map))
        logit.log("campaign_overview: generated_map: %s" % repr(generated_map))

        campaign = (
            ctx.db.query(Campaign).filter(Campaign.campaign_id == campaign_id, Campaign.experiment == ctx.experiment).first()
        )

        if not campaign:
            raise KeyError("Cannot find Campaign with id %s" % campaign_id)

        csl = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_id == campaign.campaign_id).all()

        pct_complete = 0.0
        c_ids = deque()
        for c_s in csl:
            c_ids.append(c_s.campaign_stage_id)
            pct_complete += c_s.completion_pct
        if len(csl) > 0:
            pct_complete = pct_complete/len(csl)

        logit.log(logit.INFO, "campaign_deps: c_ids=%s" % repr(c_ids))

        res = []
        res.append('<div id="overview" style="font-size: larger;"></div>')
        res.append('<script type="text/javascript">')

        res.append('var container = document.getElementById("overview");')
        res.append("var fss = window.getComputedStyle(container, null).getPropertyValue('font-size');")
        res.append("var fs = parseFloat(fss);")
        res.append("var nodes = new vis.DataSet([")

        # put a dataset object (id==0) left of the lead campaign stage

        try:
            pos = campaign.defaults["positions"][stages[0]]
        except:
            pos = {"x": 100, "y": 100}

        dstxt = "\\n".join(dslist).replace('"', "'")

        res.append(
            '  {id: %d, shape: "box", label: "%s:\\n%s\\n%d files", x: %d, y: %d, font: {size: fs}},'
            % (0, campaign.name, dstxt, int(total), pos["x"] - 500, pos["y"])
        )

        for c_s in csl:
            if campaign and campaign.defaults and 'positions' in campaign.defaults.values() and campaign.defaults.get("positions", {}).get(c_s.name, None):
                pos = campaign.defaults["positions"][c_s.name]
                res.append(
                    '  {id: %d, label: "%s", x: %d, y: %d, font: {size: fs}},'
                    % (c_s.campaign_stage_id, c_s.name, pos["x"], pos["y"])
                )
            else:
                res.append('  {id: %d, label: "%s", font: {size: fs}},' % (c_s.campaign_stage_id, c_s.name))
        res.append("]);")

        res.append("var edges = new vis.DataSet([")

        c_dl = ctx.db.query(CampaignDependency).filter(CampaignDependency.needs_campaign_stage_id.in_(c_ids)).all()

        # first an edge from the dataset to the first stage
        if len(stages) > 0:
            res.append("  {from: %d, to: %d, arrows: 'to', label: '%d file(s) submitted', length:100 }," % (0, stages[0], int(total)))

        # then all the actual dependencies
        total_consumed = 0.0
        for c_d in c_dl:
            if c_d.needs_campaign_stage_id and c_d.provides_campaign_stage_id:
                consumed = consumed_map.get(c_d.needs_campaign_stage_id, 0.0)
                generated = generated_map.get(c_d.needs_campaign_stage_id, 0.0)
                if consumed:
                    total_consumed += consumed
                    pct = 0.0
                    if total > 0:
                        pct = consumed/total
                    res.append(
                        "  {from: %d, to: %d, arrows: 'to', label: '%d file(s) consumed - %3.2f%s'},"
                        % (c_d.needs_campaign_stage_id, c_d.provides_campaign_stage_id ,consumed, (pct*100), "%")
                    )
                else:
                    res.append("  {from: %d, to: %d, arrows: 'to'}," % (c_d.needs_campaign_stage_id, c_d.provides_campaign_stage_id))


        res.append("]);")
        res.append("var data = {nodes: nodes, edges: edges};")
        res.append("var options = {")
        res.append("  manipulation: { enabled: false },")
        res.append("  height: '450px',"),
        res.append("  interaction: { zoomView: false },")
        res.append("  layout: {")
        res.append("      hierarchical: {")
        res.append("         direction: 'LR',")
        res.append("         sortMethod: 'directed',")
        res.append("         nodeSpacing: 150,")
        res.append("         parentCentralization: false")
        res.append("      }")
        res.append("   }")
        res.append("};")
        res.append("var network = new vis.Network(container, data, options);")
        res.append("var dests={")
        res.append("%s: '', " % 0)
        for c_s in csl:
            res.append(
                "%s:  '%s/campaign_stage_info/%s/%s?campaign_stage_id=%s',"
                % (c_s.campaign_stage_id, self.poms_service.path, ctx.experiment, ctx.role, c_s.campaign_stage_id)
            )
        res.append("};")
        res.append(
            "network.on('click', function(params) { if (!params || !params['nodes']||!params['nodes'][0]){ return; } ; document.location = dests[params['nodes'][0]];});"
        )
        res.append("setTimeout(() => {$('#tot_consumed').html('Total Consumed: %d');$('#consumed_pct').html('Consumed pct: %3.2f%s');$('#pct_complete').html('Pct Complete: %3.2f%s');}, 100);" % (total_consumed, ((total_consumed/total) * 100) if total != 0 else 0, "%", pct_complete, "%"))
        res.append("</script>")

        return campaign, "\n".join(res), sp_list

    def show_watching(self, ctx):
        #self.poms_service.submissionsPOMS.wrapup_tasks(ctx)
        experimenter_id = ctx.get_experimenter().experimenter_id

        logit.log(logit.INFO, "entering show_watching: %s" % experimenter_id)

        
        watching = ctx.db.query(ExperimentersWatching.campaign_obj, Campaign).filter(
            ExperimentersWatching.experimenter_id == experimenter_id, 
            Campaign.experiment == ctx.experiment,
            Campaign.creator_role == ctx.role
        ).all()

        watching = [x[1] for x in watching if x[0]]

        res_list = []
        res_scripts = []
        i = 0
        logit.log("watching_vals: " + repr(watching))
        for campaign in watching:

            logit.log(logit.INFO, "entering show_watching_"+str(i)+": %s" % campaign.campaign_id)

            stages = self.get_leading_stages(ctx, campaign.campaign_id)

            logit.log(logit.INFO, "leading stages: %s" % repr(stages))

            if len(stages) > 0:
                lead = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == stages[0]).first()
            else:
                lead = None

            exp = ctx.db.query(Campaign.experiment).filter(Campaign.campaign_id == campaign.campaign_id).first()

            #
            # for now we're skipping multiparam datasets
            # and stagedfiles ones; they are expandable though...
            #
            if lead:
                dslist = (
                    ctx.db.query(distinct(CampaignStageSnapshot.dataset))
                    .filter(CampaignStageSnapshot.campaign_stage_id == lead.campaign_stage_id)
                    .filter(CampaignStageSnapshot.cs_split_type != "multiparam")
                    .all()
                )

                dslist = [x[0] for x in dslist if x[0]]
            else:
                dslist = []

            sp_list = (
                ctx.db.query(Campaign.campaign_id, CampaignStage, Submission)
                .filter(CampaignStage.campaign_id == campaign.campaign_id, Campaign.campaign_id ==  campaign.campaign_id, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
                .order_by(CampaignStage.campaign_stage_id, Submission.submission_id)
                .all()
            )
            
            sp_list = [[x[1].name, x[2]] for x in sp_list if x[2]]

            # we start total very small so we don't divide by zero later if
            # there arent any..
            total = 0.0
           
            for ds in dslist:
                count = ctx.sam.count_files(exp, "defname:%s" % ds)
                if count > 0:
                    total += count
            if total == 0:
                subs = [x[1] for x in sp_list if x[1]]
                for sub in subs:
                    if sub.files_generated:
                        total += sub.files_generated
        

            logit.log(logit.DEBUG, "campaign_overview: total: %d" % total)

           

            counts = (
                ctx.db.query(
                    CampaignStage.campaign_stage_id, func.sum(Submission.files_consumed), func.sum(Submission.files_generated)
                )
                .filter(CampaignStage.campaign_id == campaign.campaign_id, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
                .group_by(CampaignStage.campaign_stage_id)
                .all()
            )
            consumed_map = {}
            generated_map = {}

            for r in counts:
                consumed_map[r[0]] = r[1]
                generated_map[r[0]] = r[2]

            logit.log(logit.DEBUG, "show_watching: sub_list: %s" % sp_list)
            logit.log(logit.DEBUG, "show_watching: consumed_map: %s" % consumed_map)
            logit.log(logit.DEBUG, "show_watching: generated_map: %s" % generated_map)


            #csl = campaign.stages
            csl = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_id == campaign.campaign_id).all()

            pct_complete = 0.0
            c_ids = deque()
            for c_s in csl:
                c_ids.append(c_s.campaign_stage_id)
                pct_complete += c_s.completion_pct
            if len(csl) > 0:
                pct_complete = pct_complete/len(csl)

            
            res = []
            res.append('<div id="watching_'+ str(i) +'" style="font-size: larger;"></div>')
            res.append('<script type="text/javascript">')

            res.append('var container_'+ str(i) +' = document.getElementById("watching_' + str(i) + '");')
            res.append("var fss_"+ str(i) +" = window.getComputedStyle(container_"+ str(i) +", null).getPropertyValue('font-size');")
            res.append("var fs_"+ str(i) +" = parseFloat(fss_"+ str(i) +");")
            res.append("var nodes_"+str(i)+" = new vis.DataSet([")

            # put a dataset object (id==0) left of the lead campaign stage

            try:
                pos = campaign.defaults["positions"][csl[0]]
            except:
                pos = {"x": 100, "y": 100}

            dstxt = "\\n".join(dslist).replace('"', "'")

            res.append(
                '  {id: %d, shape: "box", label: "%s:\\n%s\\n%d files", x: %d, y: %d, font: {size: fs_%d}},'
                % (0, campaign.name, dstxt, int(total), pos["x"] - 500, pos["y"], i)
            )

            for c_s in csl:
                if campaign and campaign.defaults and 'positions' in campaign.defaults.values() and campaign.defaults.get("positions", {}).get(c_s.name, None):
                    pos = campaign.defaults["positions"][c_s.name]
                    res.append(
                        '  {id: %d, label: "%s", x: %d, y: %d, font: {size: fs_%d}},'
                        % (c_s.campaign_stage_id, c_s.name, pos["x"], pos["y"],i)
                    )
                else:
                    res.append('  {id: %d, label: "%s", font: {size: fs_%d}},' % (c_s.campaign_stage_id, c_s.name, i))
            res.append("]);")

            res.append("var edges_"+ str(i) +" = new vis.DataSet([")

            c_dl = ctx.db.query(CampaignDependency).filter(CampaignDependency.needs_campaign_stage_id.in_(c_ids)).all()

            # first an edge from the dataset to the first stage
            if len(stages) > 0:
                res.append("  {from: %d, to: %d, arrows: 'to', label: '%d file(s) submitted', length:100 }," % (0, stages[0], int(total)))

            # then all the actual dependencies
            total_consumed = 0.0
            for c_d in c_dl:
                if c_d.needs_campaign_stage_id and c_d.provides_campaign_stage_id:
                    consumed = consumed_map.get(c_d.needs_campaign_stage_id, 0.0)
                    if consumed:
                        total_consumed += consumed
                        pct = 0.0
                        if total > 0:
                            pct = consumed/total
                        res.append(
                            "  {from: %d, to: %d, arrows: 'to', label: '%d file(s) consumed - %3.2f%s'},"
                            % (c_d.needs_campaign_stage_id, c_d.provides_campaign_stage_id, consumed, (pct * 100), "%")
                        )
                    else:
                        res.append("  {from: %d, to: %d, arrows: 'to'}," % (c_d.needs_campaign_stage_id, c_d.provides_campaign_stage_id))


            res.append("]);")
            res.append("var data_"+ str(i) +" = {nodes: nodes_"+ str(i) +", edges: edges_"+ str(i) +"};")
            res.append("var options_"+ str(i) +" = {")
            res.append("  manipulation: { enabled: false },")
            res.append("  height: '300px',"),
            res.append("  interaction: { zoomView: false },")
            res.append("  layout: {")
            res.append("      hierarchical: {")
            res.append("         direction: 'LR',")
            res.append("         sortMethod: 'directed',")
            res.append("         nodeSpacing: 150,")
            res.append("         parentCentralization: false")
            res.append("      }")
            res.append("   }")
            res.append("};")
            res.append("var network_"+ str(i) +" = new vis.Network(container_"+ str(i) +", data_"+ str(i) +", options_"+ str(i) +");")
            res.append("var dests_"+ str(i) +"={")
            res.append("%s: '', " % 0)
            for c_s in csl:
                res.append(
                    "%s:  '%s/campaign_stage_info/%s/%s?campaign_stage_id=%s',"
                    % (c_s.campaign_stage_id, self.poms_service.path, ctx.experiment, ctx.role, c_s.campaign_stage_id)
                )
            res.append("};")
            res.append(
                "network_"+ str(i) +".on('click', function(params) {if (!params || !params['nodes']||!params['nodes'][0]){ return; } ; window.open(dests_"+ str(i) +"[params['nodes'][0]], '_blank').focus(); })"
            )
            res.append("setTimeout(() => {$('#tot_consumed_%s').html('Total Consumed: %d');$('#consumed_pct_%s').html('Consumed pct: %3.2f%s'); $('#pct_complete_%s').html('Pct Complete: %3.2f%s');}, 100);" % (str(i),total_consumed, str(i), ((total_consumed/total) * 100) if total != 0 else 0, "%",  str(i), pct_complete, "%"))
            res.append("</script>")
            i += 1
            res_list.append("\n".join(res))
        return res_list, watching

    # h3. launch_campaign
    def launch_campaign(
        self,
        ctx,
        campaign_id,
        launcher=None,
        dataset_override=None,
        parent_submission_id=None,
        param_overrides=None,
        test_login_setup=None,
        test_launch=False,
        output_commands=False
    ):
        """
            Find the starting stage in a campaign, and launch it with
            launch_jobs(campaign_stage_id=...)
        """
        stages = self.get_leading_stages(ctx, campaign_id)
        logit.log("launch_campaign: got stages %s" % repr(stages))
        if len(stages) == 1:
            return self.poms_service.submissionsPOMS.launch_jobs(
                ctx,
                stages[0],
                launcher,
                dataset_override,
                parent_submission_id,
                param_overrides,
                test_login_setup,
                test_launch,
                output_commands, 
            )
        raise AssertionError("Cannot determine which stage in campaign to launch of %d candidates" % len(stages))

    # h3. make_test_campaign_for
    def make_test_campaign_for(self, ctx, campaign_def_id, campaign_def_name):
        """
            Build a test_campaign for a given campaign definition
        """
        experimenter_id = ctx.get_experimenter()

        c_s = (
            ctx.db.query(CampaignStage)
            .filter(CampaignStage.job_type_id == campaign_def_id, CampaignStage.name == "_test_%s" % campaign_def_name)
            .first()
        )
        if not c_s:
            l_t = ctx.db.query(LoginSetup).filter(LoginSetup.experiment == ctx.experiment).first()
            c_s = CampaignStage()
            c_s.job_type_id = campaign_def_id
            c_s.name = "_test_%s" % campaign_def_name
            c_s.experiment = ctx.experiment
            c_s.creator = experimenter_id
            c_s.created = datetime.now(utc)
            c_s.updated = datetime.now(utc)
            c_s.vo_role = "Production" if ctx.role == "production" else "Analysis"
            c_s.creator_role = ctx.role
            c_s.dataset = ""
            c_s.login_setup_id = l_t.login_setup_id
            c_s.software_version = ""
            c_s.campaign_stage_type = "regular"
            ctx.db.add(c_s)
            ctx.db.commit()
            c_s = (
                ctx.db.query(CampaignStage)
                .filter(CampaignStage.job_type_id == campaign_def_id, CampaignStage.name == "_test_%s" % campaign_def_name)
                .first()
            )
        return c_s.campaign_stage_id

    # h3. campaign_deps_ini
    def campaign_deps_ini(self, ctx, name=None, stage_id=None, login_setup=None, job_type=None, full=None):
        """
            Generate ini-format dump of campaign and dependencies
        """
        res = []
        campaign_stages = []
        jts = set()
        lts = set()
        the_campaign = None

        if job_type is not None:
            res.append("# with job_type %s" % job_type)
            j_t = ctx.db.query(JobType).filter(JobType.name == job_type, JobType.experiment == ctx.experiment).first()
            if j_t:
                jts.add(j_t)

        if login_setup is not None:
            res.append("# with login_setup: %s" % login_setup)
            l_t = ctx.db.query(LoginSetup).filter(LoginSetup.name == login_setup, LoginSetup.experiment == ctx.experiment).first()
            if l_t:
                lts.add(l_t)

        if name is not None:
            the_campaign = ctx.db.query(Campaign).filter(Campaign.name == name, Campaign.experiment == ctx.experiment).scalar()

            if the_campaign is None:
                return f"Error: Campaign '{name}' was not found for '{ctx.experiment}' experiment"
            #
            # campaign_stages = ctx.db.query(CampaignStage).join(Campaign).filter(
            #     Campaign.name == name,
            #     CampaignStage.campaign_id == Campaign.campaign_id).all()
            campaign_stages = the_campaign.stages.all()

        if stage_id is not None:
            cidl1 = (
                ctx.db.query(CampaignDependency.needs_campaign_stage_id)
                .filter(CampaignDependency.provides_campaign_stage_id == stage_id)
                .all()
            )
            cidl2 = (
                ctx.db.query(CampaignDependency.provides_campaign_stage_id)
                .filter(CampaignDependency.needs_campaign_stage_id == stage_id)
                .all()
            )
            s = set([stage_id])
            s.update(cidl1)
            s.update(cidl2)
            campaign_stages = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id.in_(s)).all()
        cnames = {}
        for c_s in campaign_stages:
            cnames[c_s.campaign_stage_id] = c_s.name

        # lookup relevant dependencies
        dmap = {}
        for cid in cnames:
            dmap[cid] = []

        fpmap = {}
        for cid in cnames:
            c_dl = (
                ctx.db.query(CampaignDependency)
                .filter(CampaignDependency.provides_campaign_stage_id == cid)
                .filter(CampaignDependency.needs_campaign_stage_id.in_(cnames.keys()))
                .all()
            )
            for c_d in c_dl:
                if c_d.needs_campaign_stage_id in cnames.keys():
                    dmap[cid].append(c_d.needs_campaign_stage_id)
                    fpmap[(cid, c_d.needs_campaign_stage_id)] = c_d.file_patterns

        # sort by dependencies(?)
        cidl = list(cnames.keys())
        for cid in cidl:
            for dcid in dmap[cid]:
                if cidl.index(dcid) < cidl.index(cid):
                    cidl[cidl.index(dcid)], cidl[cidl.index(cid)] = cidl[cidl.index(cid)], cidl[cidl.index(dcid)]

        if the_campaign:
            res.append("[campaign]")
            res.append("experiment=%s" % the_campaign.experiment)
            res.append("poms_role=%s" % the_campaign.creator_role)
            res.append("name=%s" % the_campaign.name)
            res.append("state=%s" % ("Active" if the_campaign.active else "Inactive"))
            res.append(
                "campaign_keywords=%s" % (json.dumps(the_campaign.campaign_keywords) if the_campaign.campaign_keywords else "{}")
            )
            res.append("campaign_stage_list=%s" % ",".join(map(cnames.get, cidl)))
            res.append("")

            positions = None
            defaults = the_campaign.defaults or {}
            if defaults:
                if "defaults" in defaults and "positions" in defaults:
                    positions = defaults["positions"]
                    defaults = defaults["defaults"]

                if defaults:
                    res.append("[campaign_defaults]")
                    # for (k, v) in defaults.items():
                    #     res.append("%s=%s" % (k, v))
                    res.append("vo_role=%s" % defaults.get("vo_role"))
                    # res.append("state=%s" % defaults.get("state"))
                    res.append("software_version=%s" % defaults.get("software_version"))
                    res.append("dataset_or_split_data=%s" % defaults.get("dataset"))
                    res.append("cs_split_type=%s" % defaults.get("cs_split_type"))
                    res.append("default_clear_cronjob=%s" % "True")
                    res.append("completion_type=%s" % defaults.get("completion_type"))
                    res.append("completion_pct=%s" % defaults.get("completion_pct"))
                    res.append("param_overrides=%s" % (defaults.get("param_overrides") or "[]"))
                    res.append("test_param_overrides=%s" % (defaults.get("test_param_overrides") or "[]"))
                    res.append("merge_overrides=%s" % (defaults.get("merge_overrides") or "False"))
                    res.append("login_setup=%s" % (defaults.get("login_setup") or "generic"))
                    res.append("job_type=%s" % (defaults.get("job_type") or "generic"))
                    res.append("stage_type=%s" % (defaults.get("stage_type") or "regular"))
                    res.append("output_ancestor_depth=%s" % (defaults.get("output_ancestor_depth") or "1"))
                    res.append("")
                else:
                    defaults = {}

            if full in ("1", "y", "Y", "t", "T"):
                if positions:
                    res.append("[node_positions]")
                    for (n, (k, v)) in enumerate(positions.items()):
                        res.append('nxy{}=["{}", {}, {}]'.format(n, k, v["x"], v["y"]))
                    res.append("")

        for c_s in campaign_stages:
            res.append("[campaign_stage %s]" % c_s.name)
            # res.append("name=%s" % c_s.name)
            if c_s.vo_role != defaults.get("vo_role"):
                res.append("vo_role=%s" % c_s.vo_role)
            if c_s.software_version != defaults.get("software_version"):
                res.append("software_version=%s" % c_s.software_version)
            if c_s.output_ancestor_depth != defaults.get("output_ancestor_depth", 1):
                res.append("output_ancestor_depth=%s" % c_s.output_ancestor_depth)
            if c_s.dataset != defaults.get("dataset_or_split_data"):
                res.append("dataset_or_split_data=%s" % c_s.dataset)
            if c_s.cs_split_type != defaults.get("cs_split_type"):
                res.append("cs_split_type=%s" % c_s.cs_split_type)
            if c_s.completion_type != defaults.get("completion_type"):
                res.append("completion_type=%s" % c_s.completion_type)
            if str(c_s.completion_pct) != defaults.get("completion_pct"):
                res.append("completion_pct=%s" % c_s.completion_pct)
            if json.dumps(c_s.param_overrides) != defaults.get("param_overrides"):
                res.append("param_overrides=%s" % json.dumps(c_s.param_overrides or []))
            if json.dumps(c_s.test_param_overrides) != defaults.get("test_param_overrides"):
                res.append("test_param_overrides=%s" % json.dumps(c_s.test_param_overrides or []))
            if c_s.login_setup_obj.name != defaults.get("login_setup"):
                res.append("login_setup=%s" % c_s.login_setup_obj.name)
            if c_s.job_type_obj.name != defaults.get("job_type"):
                res.append("job_type=%s" % c_s.job_type_obj.name)
            res.append("merge_overrides=%s" % c_s.merge_overrides)
            res.append("stage_type=%s" % c_s.campaign_stage_type)
            res.append("default_clear_cronjob=%s" % str(c_s.default_clear_cronjob))
            jts.add(c_s.job_type_obj)
            lts.add(c_s.login_setup_obj)
            res.append("")

        for l_t in lts:
            res.append("[login_setup %s]" % l_t.name)
            res.append("host=%s" % l_t.launch_host)
            res.append("account=%s" % l_t.launch_account)
            res.append(
                "setup=%s" % (l_t.launch_setup.replace("\r", ";").replace("\n", ";").replace(";;", ";").replace(";;", ";"))
            )
            res.append("")

        for j_t in jts:
            res.append("[job_type %s]" % j_t.name)
            res.append("launch_script=%s" % j_t.launch_script)
            res.append("parameters=%s" % json.dumps(j_t.definition_parameters))
            res.append("output_file_patterns=%s" % j_t.output_file_patterns)
            res.append("recoveries = %s" % json.dumps(self.poms_service.miscPOMS.get_recoveries(ctx, j_t.job_type_id)))
            res.append("")

        # still need dependencies
        for cid in dmap:
            if not dmap[cid]:
                continue
            res.append("[dependencies %s]" % cnames[cid])
            i = 0
            for dcid in dmap[cid]:
                i = i + 1
                res.append("campaign_stage_%d = %s" % (i, cnames[dcid]))
                res.append("file_pattern_%d = %s" % (i, fpmap[(cid, dcid)]))
            res.append("")

        res.append("")

        return "\n".join(res)

    # h3. campaign_deps_svg
    def campaign_deps_svg(self, ctx, campaign_name=None, campaign_stage_id=None):
        """
            this used to use "dot" to generate an svg diagram, now
            we use vis.js Network in javascript.
        """
        if campaign_name is not None:
            campaign = (
                ctx.db.query(Campaign).filter(Campaign.name == campaign_name, Campaign.experiment == ctx.experiment).first()
            )

            if not campaign:
                raise KeyError("Cannot find Campaign with name %s" % campaign_name)

            csl = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_id == campaign.campaign_id).all()

            csl = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_id == campaign.campaign_id).all()
        else:
            campaign = None

        if campaign_stage_id is not None:
            s = set([cs[0] if cs[1] == campaign_stage_id else cs[1]  for cs in  (
                ctx.db.query(CampaignDependency.needs_campaign_stage_id, CampaignDependency.provides_campaign_stage_id)
                .filter(or_(
                    CampaignDependency.provides_campaign_stage_id == campaign_stage_id,
                    CampaignDependency.needs_campaign_stage_id == campaign_stage_id
                ))
                .all()
            )])
            s.add(campaign_stage_id)
            
            csl = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id.in_(s)).all()

        c_ids = deque()
        for c_s in csl:
            c_ids.append(c_s.campaign_stage_id)

        logit.log(logit.INFO, "campaign_deps: c_ids=%s" % repr(c_ids))

        res = []
        res.append('<div id="dependencies" style="font-size: larger;"></div>')
        res.append('<script type="text/javascript">')

        res.append('var container = document.getElementById("dependencies");')
        res.append("var fss = window.getComputedStyle(container, null).getPropertyValue('font-size');")
        res.append("var fs = parseFloat(fss);")
        res.append("var nodes = new vis.DataSet([")

        for c_s in csl:
            if campaign and campaign.defaults and 'positions' in campaign.defaults.values() and campaign.defaults.get("positions", {}).get(c_s.name, None):
                pos = campaign.defaults["positions"][c_s.name]
                res.append(
                    '  {id: %d, label: "%s", x: %d, y: %d, font: {size: fs}},'
                    % (c_s.campaign_stage_id, c_s.name, pos["x"], pos["y"])
                )
            else:
                res.append('  {id: %d, label: "%s", font: {size: fs}},' % (c_s.campaign_stage_id, c_s.name))
        res.append("]);")

        res.append("var edges = new vis.DataSet([")

        c_dl = ctx.db.query(CampaignDependency).filter(CampaignDependency.needs_campaign_stage_id.in_(c_ids)).all()

        for c_d in c_dl:
            res.append("  {from: %d, to: %d, arrows: 'to'}," % (c_d.needs_campaign_stage_id, c_d.provides_campaign_stage_id))

        res.append("]);")
        res.append("var data = {nodes: nodes, edges: edges};")
        res.append("var options = {")
        res.append("  manipulation: { enabled: false },")
        res.append("  height: '%dpx'," % (200 + 50 * len(c_ids))),
        res.append("  interaction: { zoomView: false },")
        res.append("  layout: {")
        res.append("      hierarchical: {")
        res.append("         direction: 'LR',")
        res.append("         sortMethod: 'directed'")
        res.append("      }")
        res.append("   }")
        res.append("};")
        res.append("var network = new vis.Network(container, data, options);")
        res.append("var dests={")
        for c_s in csl:
            res.append(
                "%s:  '%s/campaign_stage_info/%s/%s?campaign_stage_id=%s',"
                % (c_s.campaign_stage_id, self.poms_service.path, ctx.experiment, ctx.role, c_s.campaign_stage_id)
            )
        res.append("};")
        res.append(
            "network.on('click', function(params) { if (!params || !params['nodes']||!params['nodes'][0]){ return; } ; document.location = dests[params['nodes'][0]];})"
        )
        res.append("</script>")
        return "\n".join(res)

    # h3. show_campaigns
    def show_campaigns(self, ctx, **kwargs):
        """
            Return data for campaigns table for current experiment, etc.
        """
        experimenter = ctx.get_experimenter()
        action = kwargs.get("action", None)
        msg = "OK"
        if action == "delete":
            name = kwargs.get("del_campaign_name")
            if kwargs.get("pcl_call") in ("1", "t", "True", "true"):
                campaign_id = (
                    ctx.db.query(Campaign.campaign_id)
                    .filter(
                        Campaign.experiment == ctx.experiment,
                        Campaign.creator == experimenter.experimenter_id,
                        Campaign.name == name,
                    )
                    .scalar()
                )
            else:
                campaign_id = kwargs.get("del_campaign_id")
            if not campaign_id:
                msg = f"The campaign '{name}' does not exist."
                return None, "", msg, None
            self.poms_service.permissions.can_modify(ctx, "Campaign", item_id=campaign_id)

            campaign = ctx.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).scalar()
            subs = (
                ctx.db.query(Submission)
                .join(CampaignStage, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
                .filter(CampaignStage.campaign_id == campaign_id)
            )
            if subs.count() > 0:
                msg = "This campaign has been submitted.  It cannot be deleted."
            else:
                # Delete all dependency records for all campaign stages
                ctx.db.query(CampaignDependency).filter(
                    or_(
                        CampaignDependency.provider.has(CampaignStage.campaign_id == campaign_id),
                        CampaignDependency.consumer.has(CampaignStage.campaign_id == campaign_id),
                    )
                ).delete(synchronize_session=False)
                # Delete all campaign stages
                campaign.stages.delete()
                # Delete all campaign tag records
                ctx.db.query(CampaignsTag).filter(CampaignsTag.campaign_id == campaign_id).delete()
                # Delete the campaign
                ctx.db.delete(campaign)
                ctx.db.commit()
                msg = ("Campaign named %s with campaign_id %s " "and related CampagnStages were deleted.") % (
                    kwargs.get("del_campaign_name"),
                    campaign_id,
                )
            if kwargs.get("pcl_call") in ("1", "t", "True", "true"):
                return None, "", msg, None

        data = {}
        q = (
            ctx.db.query(Campaign)
            .options(joinedload(Campaign.experimenter_creator_obj))
            .filter(Campaign.experiment == ctx.experiment)
            .order_by(Campaign.name)
        )
        watching = (
            ctx.db.query(ExperimentersWatching)
            .filter(ExperimentersWatching.experimenter_id == experimenter.experimenter_id )
            .all()
        )

        data["watching"] = [x.campaign_id for x in watching]

        if kwargs.get("update_view", None) is None:
            # view flags not specified, use defaults
            data["view_active"] = "view_active"
            data["view_inactive"] = None
            data["view_mine"] = experimenter.experimenter_id
            data["view_others"] = experimenter.experimenter_id
            data["view_analysis"] = "view_analysis" if ctx.role in ("analysis", "superuser") else None
            data["view_production"] = "view_production" if ctx.role in ("production", "production-shifter", "superuser") else None
        else:
            data["view_active"] = kwargs.get("view_active", None)
            data["view_inactive"] = kwargs.get("view_inactive", None)
            data["view_mine"] = kwargs.get("view_mine", None)
            data["view_others"] = kwargs.get("view_others", None)
            data["view_analysis"] = kwargs.get("view_analysis", None)
            data["view_production"] = kwargs.get("view_production", None)

        if data["view_analysis"] and data["view_production"]:
            pass
        elif data["view_analysis"]:
            q = q.filter(Campaign.creator_role == "analysis")
        elif data["view_production"]:
            q = q.filter(Campaign.creator_role == "production")

        if data["view_mine"] and data["view_others"]:
            pass
        elif data["view_mine"]:
            q = q.filter(Campaign.creator == experimenter.experimenter_id)
        elif data["view_others"]:
            q = q.filter(Campaign.creator != experimenter.experimenter_id)

        if data["view_active"] and data["view_inactive"]:
            pass
        elif data["view_active"]:
            q = q.filter(Campaign.active.is_(True))
        elif data["view_inactive"]:
            q = q.filter(Campaign.active.is_(False))

        csl = q.all()

        if not csl:
            return csl, "", msg, data

        data["authorized"] = []
        data["authorized_shifter"] = []
        for c_s in csl:
            # permissions module raises exceptions when not authorized,
            # so we have to use a try: block to decide if we're authorized
            try:
                self.poms_service.permissions.can_modify(ctx, "Campaign", item_id=c_s.campaign_id)
                data["authorized"].append(True)
            except:
                data["authorized"].append(False)

            try:
                self.poms_service.permissions.can_do(ctx, "Campaign", item_id=c_s.campaign_id)
                data["authorized_shifter"].append(True)
            except:
                data["authorized_shifter"].append(True)

        last_activity_l = (
            ctx.db.query(func.max(Submission.updated))
            .join(CampaignStage, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
            .join(Campaign, CampaignStage.campaign_id == Campaign.campaign_id)
            .filter(Campaign.experiment == ctx.experiment)
            .first()
        )
        last_activity = ""
        if last_activity_l and last_activity_l[0]:
            if datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
                last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        return csl, last_activity, msg, data

    # h3. make_stale_campaigns_inactive
    def make_stale_campaigns_inactive(self, ctx):
        """
            turn off active flag on campaigns without recent activity
        """
        # Get stages run in the last 7 days - active stage list
        lastweek = datetime.now(utc) - timedelta(days=7)
        recent_sq = ctx.db.query(distinct(Submission.campaign_stage_id)).filter(Submission.created > lastweek)
        # Get the campaign_id of the active stages - active campaign list
        active_campaigns = ctx.db.query(distinct(CampaignStage.campaign_id)).filter(
            CampaignStage.campaign_stage_id.in_(recent_sq)
        )
        # Turn off the active flag on stale campaigns.
        stale = (
            ctx.db.query(Campaign)
            .filter(Campaign.active.is_(True), Campaign.campaign_id.notin_(active_campaigns))
            .update({"active": False}, synchronize_session=False)
        )

        ctx.db.commit()
        return []

    def watch_campaign(self, ctx, **kwargs):
        data = {}
        experimenter_id = ctx.get_experimenter().experimenter_id
        selected = kwargs.get("selected", False) in ["true", "True"]
        campaign_id = kwargs.get("campaign_id", None)
        logit.log("watch_campaign: selected = %s" % str(selected))
        try:
            if(selected):
                experimenter_watching = ExperimentersWatching(
                    experimenters_watching_id = uuid.uuid4(),
                    experimenter_id = experimenter_id, 
                    campaign_id = campaign_id,
                    created=datetime.now(utc)
                    )
                ctx.db.add(experimenter_watching)
                ctx.db.commit()
            else: 
                watching = ctx.db.query(ExperimentersWatching).filter(
                    ExperimentersWatching.campaign_id == campaign_id, 
                    ExperimentersWatching.experimenter_id == experimenter_id).first()
                if watching:
                    ctx.db.delete(watching)
                    ctx.db.commit()


            data["message"] = "Success"
        except SQLAlchemyError as exc:
            data["message"] = "SQLAlchemyError: " "Please report this to the administrator. " "Message: %s" % " ".join(exc.args)
            logit.log(" ".join(exc.args))
            ctx.db.rollback()
        else:
            ctx.db.commit()
        return json.dumps(data)
        

    # h3. save_campaign
    def save_campaign(self, ctx, replace=False, pcl_call=0, *args, **kwargs):

        exp = ctx.experiment
        role = ctx.role

        experimenter = ctx.get_experimenter()
        user_id = experimenter.experimenter_id

        data = kwargs.get("form", None)
        logit.log("save_campaigns: data: %s" % data)
        everything = json.loads(data)
        message = []

        # check permissions here, because we need to parse the json
        # to tell
        stages = everything["stages"]
        campaign = [s for s in stages if s.get("id").startswith("campaign ")][0]
        c_old_name = campaign.get("id").split(" ", 1)[1]
        c_new_name = campaign.get("label")

        # permissions check, deferred from top level...
        self.poms_service.permissions.can_view(ctx, "Campaign", name=c_old_name, experiment=ctx.experiment)
        self.poms_service.permissions.can_modify(ctx, "Campaign", name=c_new_name, experiment=ctx.experiment)

        # Process job types and login setups first
        misc = everything["misc"]
        for el in misc:
            eid = el.get("id")
            old_name = eid.split(" ", 1)[1]
            new_name = el.get("label")
            clean = el.get("clean")
            form = el.get("form")
            #
            if not clean:
                name = new_name
                if eid.startswith("job_type "):
                    definition_parameters = form.get("parameters")
                    if definition_parameters:
                        definition_parameters = json.loads(definition_parameters)

                    job_type = JobType(
                        name=name,
                        experiment=ctx.experiment,
                        output_file_patterns=form.get("output_file_patterns"),
                        launch_script=form.get("launch_script"),
                        definition_parameters=definition_parameters,
                        creator=user_id,
                        created=datetime.now(utc),
                        creator_role=role,
                    )
                    ctx.db.add(job_type)
                    try:
                        ctx.db.commit()
                        recoveries = form.get("recoveries")
                        if recoveries:
                            self.poms_service.miscPOMS.fixup_recoveries(ctx, job_type.job_type_id, recoveries)
                        ctx.db.commit()
                    except IntegrityError:
                        message.append(f"Warning: JobType '{name}' already exists and will not change.")
                        logit.log(f"*** DB error: {message}")
                        ctx.db.rollback()

                elif eid.startswith("login_setup "):
                    login_setup = LoginSetup(
                        name=name,
                        experiment=ctx.experiment,
                        launch_host=form.get("host"),
                        launch_account=form.get("account"),
                        launch_setup=form.get("setup"),
                        creator=user_id,
                        created=datetime.now(utc),
                        creator_role=role,
                    )
                    ctx.db.add(login_setup)
                    try:
                        print(f"*** Creating: LoginSetup '{name}'.")
                        ctx.db.flush()
                        ctx.db.commit()
                    except IntegrityError:
                        message.append(f"Warning: LoginSetup '{name}' already exists and will not change.")
                        print(f"*** DB error: {message}")
                        ctx.db.rollback()

        # Now process all stages
        campaign_clean = campaign.get("clean")
        defaults = campaign.get("form")
        campaign_keywords_json = defaults.get("campaign_keywords", "")
        if not campaign_keywords_json:
            campaign_keywords_json = "{}"
        campaign_keywords = json.loads(campaign_keywords_json)
        logit.log("saw campaign_keywords: %s" % repr(campaign_keywords))
        if "campaign_keywords" in defaults:
            del defaults["campaign_keywords"]
        position = campaign.get("position")

        the_campaign = ctx.db.query(Campaign).filter(Campaign.name == c_old_name, Campaign.experiment == ctx.experiment).scalar()
        if the_campaign:
            # the_campaign.defaults = defaults    # Store the defaults unconditionally as they may be not stored yet
            the_campaign.defaults = {
                "defaults": defaults,
                "positions": position,
            }  # Store the defaults unconditionally as they may be not be stored yet
            if c_new_name != c_old_name:
                the_campaign.name = c_new_name

            the_campaign.campaign_keywords = campaign_keywords
            ctx.db.add(the_campaign)

        else:  # we do not have a campaign in the db for this experiment so create the campaign and then do the linking
            the_campaign = Campaign()
            the_campaign.name = c_new_name
            the_campaign.defaults = {"defaults": defaults, "positions": position}
            the_campaign.experiment = ctx.experiment
            the_campaign.creator = user_id
            the_campaign.creator_role = role
            the_campaign.campaign_keywords = campaign_keywords
            ctx.db.add(the_campaign)
        ctx.db.commit()

        old_stages = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_obj.has(Campaign.name == c_new_name)).all()
        old_stage_names = set([s.name for s in old_stages])
        logit.log("############## old_stage_names: {}".format(old_stage_names))

        # new_stages = tuple(filter(lambda s: not s.get('id').startswith('campaign '), stages))
        new_stages = [s for s in stages if not s.get("id").startswith("campaign ")]
        new_stage_names = set([s.get("id") for s in new_stages])
        logit.log("############## new_stage_names: {}".format(new_stage_names))

        deleted_stages = old_stage_names - new_stage_names
        if deleted_stages:
            # (ctx.db.query(CampaignStage)                    # We DON'T delete the stages
            #  .filter(CampaignStage.name.in_(deleted_stages))
            #  .delete(synchronize_session=False))
            cs_list = (
                ctx.db.query(CampaignStage)
                .filter(CampaignStage.name.in_(deleted_stages))
                .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
            )  # Get the stage list
            for c_s in cs_list:
                c_s.campaign_id = None  # Detach the stage from campaign
            ctx.db.query(CampaignDependency).filter(
                or_(
                    CampaignDependency.provider.has(CampaignStage.name.in_(deleted_stages)),
                    CampaignDependency.consumer.has(CampaignStage.name.in_(deleted_stages)),
                ),
                or_(
                    CampaignDependency.provider.has(CampaignStage.campaign_id == the_campaign.campaign_id),
                    CampaignDependency.consumer.has(CampaignStage.campaign_id == the_campaign.campaign_id),
                ),
            ).delete(
                synchronize_session=False
            )  # Delete the stage dependencies if any
            ctx.db.commit()

        for stage in new_stages:
            old_name = stage.get("id")
            logit.log("save_campaign: for stage loop: %s" % old_name)
            new_name = stage.get("label")
            position = stage.get("position")  # Ignore for now
            clean = stage.get("clean")
            form = stage.get("form")
            # Use the field if provided otherwise use defaults
            # form = {k: (form[k] or defaults[k]) for k in form}
            keys = set(defaults.keys()) | set(form.keys())
            form = {k: (form.get(k) or defaults.get(k)) for k in keys}
            print("############## i: '{}', l: '{}', c: '{}', f: '{}', p: '{}'".format(old_name, new_name, clean, form, position))

            active = form.pop("state", None) in ("True", "true", "1", "Active")

            completion_pct = form.pop("completion_pct")
            completion_type = form.pop("completion_type")
            split_type = form.pop("cs_split_type", None)
            default_clear_cronjob = form.pop("default_clear_cronjob", True)
            dataset = form.pop("dataset_or_split_data")
            job_type = form.pop("job_type")
            print("################ job_type: '{}'".format(job_type))
            login_setup = form.pop("login_setup")
            print("################ login_setup: '{}'".format(login_setup))
            param_overrides = form.pop("param_overrides", None) or "[]"
            print("################ param_overrides: '{}'".format(param_overrides))
            if param_overrides:
                param_overrides = json.loads(param_overrides)
            merge_overrides = form.pop("merge_overrides", False)
            software_version = form.pop("software_version")
            test_param_overrides = form.pop("test_param_overrides", None)
            test_param_overrides = json.loads(test_param_overrides) if test_param_overrides else None
            output_ancestor_depth = form.pop("output_ancestor_depth", 1)

            vo_role = form.pop("vo_role")

            stage_type = form.pop("stage_type", "regular")

            login_setup_id = (
                ctx.db.query(LoginSetup.login_setup_id)
                .filter(LoginSetup.experiment == ctx.experiment)
                .filter(LoginSetup.name == login_setup)
                .scalar()
                or ctx.db.query(LoginSetup.login_setup_id)
                .filter(LoginSetup.experiment == "samdev")
                .filter(LoginSetup.name == login_setup)
                .scalar()
            )
            if not login_setup_id:
                logit.log("save_campaign: Error: bailing on not login_setup_id login_id '%s'" % login_setup)
                message.append(f"Error: LoginSetup '{login_setup}' not found! Campaign is incomplete!")
                return {"status": "400 Bad Request", "message": message}

            job_type_id = (
                ctx.db.query(JobType.job_type_id)
                .filter(JobType.experiment == ctx.experiment)
                .filter(JobType.name == job_type)
                .scalar()
                or ctx.db.query(JobType.job_type_id)
                .filter(JobType.experiment == "samdev")
                .filter(JobType.name == job_type)
                .scalar()
            )
            if not job_type_id:
                logit.log("save_campaign: Error bailing on not job_type_id: job_type: '%s'" % job_type)
                message.append(f"Error: JobType '{job_type}' not found! Campaign is incomplete!")
                return {"status": "400 Bad Request", "message": message}

            logit.log("save_campaign: for stage loop: here1")
            # when we just cloned a stage, we think there's old one, but there
            # isn't , hence the if obj: below
            obj = None
            if old_name in old_stage_names:
                obj = (
                    ctx.db.query(CampaignStage)  #
                    .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                    .filter(CampaignStage.name == old_name)
                    .scalar()
                )  # Get stage by the old name
            if obj:
                logit.log("save_campaign: for stage loop: found campaign stage obj")
                obj.name = new_name  # Update the name using provided new_name
                if not clean or not campaign_clean:  # Update all fields from the form
                    obj.completion_pct = completion_pct
                    obj.completion_type = completion_type
                    obj.cs_split_type = split_type
                    obj.default_clear_cronjob = default_clear_cronjob not in (False, "False", "false")
                    obj.dataset = dataset
                    obj.job_type_id = job_type_id
                    obj.login_setup_id = login_setup_id
                    obj.param_overrides = param_overrides
                    obj.merge_overrides = merge_overrides in (True, "True", "true")
                    obj.software_version = software_version
                    obj.test_param_overrides = test_param_overrides
                    obj.output_ancestor_depth = output_ancestor_depth
                    obj.vo_role = vo_role
                    obj.campaign_stage_type = stage_type
                    obj.active = active
                    obj.updater = user_id
                    obj.updated = datetime.now(utc)

                    ctx.db.flush()
            else:  # If this is a new stage then create and store it
                logit.log("for_stage_loop: new campaign stage...")
                c_s = CampaignStage(
                    name=new_name,
                    experiment=exp,
                    campaign_id=the_campaign.campaign_id,
                    # active=active,
                    #
                    completion_pct=completion_pct,
                    completion_type=completion_type,
                    cs_split_type=split_type,
                    default_clear_cronjob = default_clear_cronjob not in (False, "False", "false"),
                    dataset=dataset,
                    job_type_id=job_type_id,
                    login_setup_id=login_setup_id,
                    param_overrides=param_overrides,
                    merge_overrides=merge_overrides in (True, "True", "true"),
                    software_version=software_version,
                    test_param_overrides=test_param_overrides,
                    output_ancestor_depth=output_ancestor_depth,
                    vo_role=vo_role,
                    #
                    creator=user_id,
                    created=datetime.now(utc),
                    creator_role=role,
                    campaign_stage_type=stage_type,
                )
                ctx.db.add(c_s)
                ctx.db.flush()

        ctx.db.commit()

        logit.log("save_campaign: for stage loop: here2")

        # Now process all dependencies
        dependencies = everything["dependencies"]
        ctx.db.query(CampaignDependency).filter(
            or_(
                CampaignDependency.provider.has(CampaignStage.campaign_id == the_campaign.campaign_id),
                CampaignDependency.consumer.has(CampaignStage.campaign_id == the_campaign.campaign_id),
            )
        ).delete(synchronize_session=False)

        for dependency in dependencies:
            from_name = dependency["fromId"]
            to_name = dependency["toId"]
            form = dependency.get("form")
            from_id = (
                ctx.db.query(CampaignStage.campaign_stage_id)
                .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                .filter(CampaignStage.experiment == exp)
                .filter(CampaignStage.name == from_name)
                .scalar()
            )
            to_id = (
                ctx.db.query(CampaignStage.campaign_stage_id)
                .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                .filter(CampaignStage.experiment == exp)
                .filter(CampaignStage.name == to_name)
                .scalar()
            )

            file_pattern = list(form.values())[0]
            dep = CampaignDependency(
                provides_campaign_stage_id=to_id, needs_campaign_stage_id=from_id, file_patterns=file_pattern
            )
            ctx.db.add(dep)
            ctx.db.flush()
        ctx.db.commit()
        logit.log("save_campaign: for stage loop: here3")
        print("+++++++++++++++ Campaign saved")
        return {
            "status": "201 Created",
            "message": message or "OK",
            "campaign_id": the_campaign.campaign_id,
            "campaign_stage_ids": [(x.campaign_stage_id, x.name) for x in the_campaign.stages],
        }

    # h3. mark_campaign_active
    def mark_campaign_active(self, ctx, campaign_id=None, is_active=None, camp_l=None, clear_cron=False):
        logit.log("camp_l={}; is_active='{}'".format(camp_l, is_active))
        auth_error = False
        campaign_ids = (campaign_id or camp_l).split(",")
        for cid in campaign_ids:

            campaign = ctx.db.query(Campaign).filter(Campaign.campaign_id == cid).first()
            if campaign:
                try:
                    self.poms_service.permissions.can_modify(ctx, "Campaign", item_id=cid)
                    auth = True
                except:
                    auth = False
                if auth:
                    campaign.active = is_active in ("True", "Active", "true", "1")
                    ctx.db.add(campaign)
                else:
                    auth_error = True

        if clear_cron and not auth_error:
            csil = ctx.db.query(CampaignStage.campaign_stage_id).filter(CampaignStage.campaign_id.in_(campaign_ids)).all()
            for csi in csil:
                self.ps.stagesPOMS.update_launch_schedule(ctx, csi, delete=True)

        ctx.db.commit()

        return auth_error
