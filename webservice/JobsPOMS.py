#!/usr/bin/env python
"""
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
"""

import os
import re
from datetime import datetime, timedelta
from sqlalchemy import func
from .poms_model import Submission, SubmissionHistory, CampaignStage, JobType
from .utc import utc
from . import logit
from .SAMSpecifics import sam_specifics


class JobsPOMS:

    pending_files_offset = 0

    # h3. __init__
    def __init__(self, poms_service):
        self.poms_service = poms_service
        self.junkre = re.compile(r".*fcl|log.*|.*\.log$|ana_hist\.root$|.*\.sh$|.*\.tar$|.*\.json$|[-_0-9]*$")

    # h3. update_
    def update_SAM_project(self, ctx, j, projname):
        logit.log("Entering update_SAM_project(%s)" % projname)
        sid = j.submission_obj.submission_id
        exp = j.submission_obj.campaign_stage_snapshot_obj.experiment
        cid = j.submission_obj.campaign_stage_snapshot_obj.campaign_stage_id
        sam_specifics(ctx).update_project_description(projname, "POMS CampaignStage %s Submission %s" % (cid, sid))

    # h3. kill_jobs
    def kill_jobs(self, ctx, campaign_id=None, campaign_stage_id=None, submission_id=None, job_id=None, confirm=None, act="kill"):
        """
            kill jobs from the campaign, stage, or particular submission
            we want to do this all with --constraint on the POMS4_XXX_ID
            values we put in the classadd, but this doesn't get set on the
            dagman jobs (currently) so we have to kill the session leader
            jobids too to be sure we get them all.
        """
        s = None
        cs = None
        group = ctx.experiment

        if not (submission_id or campaign_id or campaign_stage_id):
            raise SyntaxError("called with out submission, campaign, or stage id" % act)

        # start a query to get the session jobsub job_id's ...
        jjidq = ctx.db.query(Submission.jobsub_job_id, Submission.submission_id)

        if campaign_id:
            what = "--constraint POMS4_CAMPAIGN_ID==%s" % campaign_id
            cs = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_id == campaign_id).one()
            csids = ctx.db.query(CampaignStage.campaign_stage_id).filter(CampaignStage.campaign_id == campaign_id).first()
            csids = list(csids)
            jjidq = jjidq.filter(Submission.campaign_stage_id.in_(csids))

        if campaign_stage_id:
            what = "--constraint POMS4_CAMPAIGN_STAGE_ID==%s" % campaign_stage_id
            cs = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).one()
            jjidq = jjidq.filter(Submission.campaign_stage_id == campaign_stage_id)

        if submission_id:
            s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).one()  #
            what = "--constraint POMS4_SUBMISSION_ID==%s" % s.submission_id
            cs = s.campaign_stage_obj
            jjidq = jjidq.filter(Submission.submission_id == submission_id)

        shq = (
            ctx.db.query(
                SubmissionHistory.submission_id.label("submission_id"), func.max(SubmissionHistory.status_id).label("max_status")
            )
            .filter(SubmissionHistory.submission_id == Submission.submission_id)
            .filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4))
            .group_by(SubmissionHistory.submission_id.label("submission_id"))
        )
        sq = shq.subquery()
        logit.log("submission history query finds: %s" % repr([x for x in shq.all()]))
        jjidq = jjidq.join(sq, sq.c.submission_id == Submission.submission_id).filter(sq.c.max_status <= 4000)
        rows = jjidq.all()

        if rows:
            jjids = [x[0] for x in rows if x[0] != None]
            sids = [x[1] for x in rows if x[1] != None]
        else:
            jjids = []
            sids = []

        if jjids and jjids[0][0]:
            jidbits = "--jobid=%s" % ",".join(jjids)
        else:
            jidbits = what

        if confirm is None:
            if jidbits != what:
                what = "%s %s" % (what, jidbits)
            return what, s, campaign_stage_id, submission_id, job_id

        else:
            # finish up the jobsub job_id query, and make a --jobid=list
            # parameter out of it.

            lts = cs.login_setup_obj
            if group == "samdev":

                group = "fermilab"

            subcmd = "q"
            status_set = None
            if act == "kill":
                subcmd = "rm"
                status_set = "Removed"
            elif act == "cancel":
                subcmd = "rm"
                status_set = "Cancelled"
            elif act in ("hold", "release"):
                subcmd = act
            else:
                raise SyntaxError("called with unknown action %s" % act)

            experimenter_login = ctx.username
            role = ctx.role
            if role == "analysis":
                vaultfilename = f"vt_{ctx.experiment}_Analysis_{experimenter_login}"
                if not os.path.exists("/home/poms/uploads/%s/%s/%s" % (ctx.experiment, ctx.username, vaultfilename)):
                    vaultfilename = f"vt_{ctx.experiment}_analysis_{experimenter_login}"
                
            else:
                vaultfilename = f"vt_{ctx.experiment}_production_{experimenter_login}"
            if ctx.role == "analysis":
                sandbox = self.poms_service.filesPOMS.get_launch_sandbox(ctx)
                vaultfile = "%s/%s" % (sandbox, vaultfilename)
                proxyfile = "%s/x509up_voms_%s_Analysis_%s" % (sandbox, ctx.experiment, experimenter_login)
            else:
                sandbox = "$HOME"
                proxyfile = "/opt/%spro/%spro.Production.proxy" % (ctx.experiment, ctx.experiment)
                if ctx.experiment == "samdev":
                    vaultfile = "/home/poms/uploads/%s/%s/%s" % (ctx.experiment, ctx.username, vaultfilename)
           

            # expand launch setup %{whatever}s campaigns...
            
            launch_setup = lts.launch_setup

            launch_setup = launch_setup.strip()
            launch_setup = launch_setup.replace("\n", ";")
            launch_setup = launch_setup.strip(";")
            launch_setup = (
                "source /grid/fermiapp/products/common/etc/setups;setup poms_client -g poms31 -z /grid/fermiapp/products/common/db; "
                + launch_setup
            )
            launch_setup = (
                "cp $X509_USER_PROXY /tmp/proxy$$ && export X509_USER_PROXY=/tmp/proxy$$  && chmod 0400 $X509_USER_PROXY && ls -l $X509_USER_PROXY;"
                if ctx.role == "analysis"
                else ""
            ) + launch_setup
            launch_setup = "export X509_USER_PROXY=%s;" % proxyfile + launch_setup
            
             # Declare where a bearer token should be stored when launch host calls htgettoken
            if ctx.role == "production" and ctx.experiment == "samdev": 
                # samdev doesn't have a managed token...
                htgettokenopts = "-a %s -r default -i fermilab  --vaulttokeninfile=%s --credkey=%s" % (ctx.web_config.get("tokens", "vaultserver"), vaultfile, experimenter_login)
            elif ctx.role == "analysis":
                htgettokenopts = "-a %s -r default -i %s --vaulttokeninfile=%s --credkey=%s" % (ctx.web_config.get("tokens","vaultserver"),group, vaultfile, experimenter_login)
            else:
                htgettokenopts = "-a %s -i %s -r %s --credkey=%spro/managedtokens/%s " % (ctx.web_config.get("tokens","vaultserver"),group, ctx.role, ctx.experiment, ctx.web_config.get("tokens", "managed_tokens_server"))

 
      
        
            # Add necessary path variables - Is this necessary?           
            #if not os.environ.get('PATH','unknown').__contains__(":/usr/sbin"):
            #    os.environ["PATH"] = "%s:/usr/sbin" % os.environ.get('PATH','unknown')

            # token logic if not defined in launch script
            token_logic = [
                ("export USER=%s; " % experimenter_login) if ctx.role == "analysis" or ctx.experiment == "samdev" else "",
                "export XDG_CACHE_HOME=/tmp/%s;" % experimenter_login if ctx.role == "analysis" or ctx.experiment == "samdev" else "",
                "export HTGETTOKENOPTS=\"%s\"; " % htgettokenopts,
                "export PATH=$PATH:/opt/jobsub_lite/bin;",
                "export _condor_SEC_CREDENTIAL_STORER=/bin/true;",
                #"export BEARER_TOKEN_FILE=/tmp/token%s; " % uu,
                "htgettoken %s; " % (htgettokenopts),
                "setup jobsub_client v_lite;"
            ]
            token_string = ""
            for cmd in token_logic:
                token_string += cmd

            
        
            cmd = """
                exec 2>&1;
                export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s;
                kinit -kt /run/secrets/poms.keytab `klist -kt /run/secrets/poms.keytab | tail -1 | sed -e 's/.* //'`|| true;
                ssh %s@%s '%s; set -x; %s jobsub_%s -G %s --role %s %s ;   jobsub_%s -G %s --role %s %s ; '
            """ % (
                group,
                lts.launch_account,
                lts.launch_host,
                launch_setup,
                token_string,
                subcmd,
                group,
                cs.vo_role,
                what,
                subcmd,
                group,
                cs.vo_role,
                jidbits,
            )

            cmd = cmd % {
                "dataset": cs.dataset,
                "version": cs.software_version,
                "group": group,
                "experimenter": cs.experimenter_creator_obj.username,
                "experiment": cs.experiment,
            }

            f = os.popen(cmd, "r")
            output = f.read()
            f.close()

            if status_set:
                for sid in sids:
                    self.poms_service.submissionsPOMS.update_submission_status(ctx, sid, status_set)
            ctx.db.commit()

            return output, cs, campaign_stage_id, submission_id, job_id

    # h3. jobtype_list
    def jobtype_list(self, ctx, name=None, full=None):
        """
            Return list of all jobtypes for the experiment.
        """
        if full:
            data = (
                ctx.db.query(JobType.name, JobType.launch_script, JobType.definition_parameters, JobType.output_file_patterns)
                .filter(JobType.experiment == ctx.experiment)
                .order_by(JobType.name)
                .all()
            )
        else:
            data = ctx.db.query(JobType.name).filter(JobType.experiment == ctx.experiment).order_by(JobType.name).all()
        return [r._asdict() for r in data]
