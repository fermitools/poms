#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from collections import deque
import re
from .poms_model import Submission, SubmissionHistory, CampaignStage, JobType
from datetime import datetime
from sqlalchemy.orm import joinedload
from sqlalchemy import func, not_, and_, or_, desc
from .utc import utc
import json
import os

from . import logit
from .pomscache import pomscache, pomscache_10


class JobsPOMS:

    pending_files_offset = 0

    def __init__(self, poms_service):
        self.poms_service = poms_service
        self.junkre = re.compile(
            r'.*fcl|log.*|.*\.log$|ana_hist\.root$|.*\.sh$|.*\.tar$|.*\.json$|[-_0-9]*$')

    def update_SAM_project(self, samhandle, j, projname):
        logit.log("Entering update_SAM_project(%s)" % projname)
        sid = j.submission_obj.submission_id
        exp = j.submission_obj.campaign_stage_snapshot_obj.experiment
        cid = j.submission_obj.campaign_stage_snapshot_obj.campaign_stage_id
        samhandle.update_project_description(
            exp, projname, "POMS CampaignStage %s Submission %s" %
            (cid, sid))
        pass

    def kill_jobs(self, dbhandle, seshandle_get, campaign_id = None, campaign_stage_id=None,
                  submission_id=None, job_id=None, confirm=None, act='kill'):
        s = None
        cs = None

        e = seshandle_get('experimenter')
        se_role = e.session_role
        exp = e.session_experiment

        if campaign_id:
            what = "--constraint=POMS4_CAMPAIGN_ID==%s" % campaign_id
            cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_id == campaign_id).first()

        if campaign_stage_id:
            what = "--constraint=POMS4_CAMPAIGN_STAGE_ID==%s" % campaign_stage_id
            cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()

        if submission_id:
            s = (dbhandle.query(Submission)
                 .filter(Submission.submission_id == submission_id)
                 .first())
            what = "--constraint=POMS4_SUBMISSION_ID==%s" % s.submission_id
            cs = s.campaign_stage_obj

        if not (submission_id or campaign_id or campaign_stage_id):
            raise SyntaxError("called with out submission, campaign, or stage id" % act)

        if confirm is None:
            return what, s, campaign_stage_id, submission_id, job_id

        else:
            group = cs.experiment
            lts = cs.login_setup_obj
            if group == 'samdev':

                group = 'fermilab'

            subcmd = 'q'
            if act == 'kill':
                subcmd = 'rm'
            elif act in ('hold', 'release'):
                subcmd = act
            else:
                raise SyntaxError("called with unknown action %s" % act)

            if se_role == 'analysis':
                sandbox = self.poms_service.filesPOMS.get_launch_sandbox(basedir, seshandle_get)
                proxyfile = "$UPLOADS/x509up_voms_%s_Analysis_%s" % (exp,experimenter_login)
            else:
                sandbox = '$HOME'
                proxyfile = "/opt/%spro/%spro.Production.proxy" % (exp, exp)

            # expand launch setup %{whatever}s campaigns...

            launch_setup = lts.launch_setup

            launch_setup = launch_setup.strip()
            launch_setup = launch_setup.replace("\n", ";")
            launch_setup = launch_setup.strip(";")
            launch_setup = "source /grid/fermiapp/products/common/etc/setups;setup poms_client -g poms31 -z /grid/fermiapp/products/common/db;" + launch_setup
            launchsetup  = ("cp $X509_USER_PROXY /tmp/proxy$$ && export X509_USER_PROXY=/tmp/proxy$$  && chmod 0400 $X509_USER_PROXY && ls -l $X509_USER_PROXY;" if se_role == "analysis" else "") + launch_setup
            launch_setup = "export X509_USER_PROXY=%s;" % proxyfile + launch_setup
            cmd = """
                exec 2>&1
                export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s
                kinit -kt $HOME/private/keytabs/poms.keytab `klist -kt $HOME/private/keytabs/poms.keytab | tail -1 | sed -e 's/.* //'`|| true
                ssh %s@%s '%s; set -x; jobsub_%s -G %s --role %s %s'
            """ % (
                group,
                self.poms_service.hostname,
                lts.launch_account,
                lts.launch_host,
                launch_setup,
                subcmd,
                group,
                cs.vo_role,
                what
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

            return output, cs, campaign_stage_id, submission_id, job_id

    # This method was deleted from the main script

    def get_efficiency(self, dbhandle, id_list, tmin, tmax):

        if isinstance(id_list, str):
            id_list = [int(cid) for cid in id_list.split(',') if cid]

        mapem = self.get_efficiency_map(dbhandle, id_list, tmin, tmax)
        efflist = deque()
        for cid in id_list:
            efflist.append(mapem.get(cid, -2))

        logit.log("got list: %s" % repr(efflist))
        return efflist

    def jobtype_list(self, dbhandle, seshandle, name=None, full=None):
        """
            Return list of all jobtypes for the experiment.
        """
        exp = seshandle('experimenter').session_experiment
        #data = dbhandle.query(JobType).filter(JobType.experiment == exp).order_by(JobType.name).all()
        # return ["%s" % r for r in data]
        if full:
            data = dbhandle.query(JobType.name,
                                  JobType.launch_script,
                                  JobType.definition_parameters,
                                  JobType.output_file_patterns).filter(JobType.experiment == exp).order_by(JobType.name).all()
        else:
            data = dbhandle.query(
                JobType.name).filter(
                JobType.experiment == exp).order_by(
                JobType.name).all()
        return [r._asdict() for r in data]
