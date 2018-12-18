#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from collections import deque
import re
from .poms_model import Submission, CampaignStage, JobType
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

    def kill_jobs(self, dbhandle, campaign_stage_id=None,
                  submission_id=None, job_id=None, confirm=None, act='kill'):
        jjil = deque()
        jql = None      # FIXME: this variable is not assigned anywhere!
        s = None
        cs = None
        if campaign_stage_id is not None or submission_id is not None:
            if campaign_stage_id is not None:
                tl = dbhandle.query(Submission, Submission.id,  func.max(SubmissionHistory.status_id))
                    .join(SubmissionHistory, Submission.submission_id == SubmissionHistory.submission_id)
                    .filter(Submission.campaign_stage_id == campaign_stage_id)
                    .group_by(Submission.id)
                    .having(func.max(SubmissionHistory.status_id) <= 4000)
                    .all()
            else:
                tl = dbhandle.query(Submission).filter(
                    Submission.submission_id == submission_id).all()
            if len(tl):
                cs = tl[0].campaign_stage_snapshot_obj
                lts = tl[0].login_setup_snap_obj
                st = tl[0]
            else:
                cs = None
                lts = None

            for s in tl:
                tjid = s.jobsub_job_id
                logit.log(
                    "kill_jobs: submission_id %s -> tjid %s" %
                    (s.submission_id, tjid))
                # for submissions/campaign_stages, kill the whole group of jobs
                # by getting the leader's jobsub_job_id and taking off
                # the '.0'.
                if tjid:
                    jjil.append(tjid.replace('.0', ''))

        if confirm is None:
            jijatem = 'kill_jobs_confirm.html'

            return jjil, st, campaign_stage_id, submission_id, job_id
        elif cs:
            group = cs.experiment
            if group == 'samdev':
                group = 'fermilab'

            subcmd = 'q'
            if act == 'kill':
                subcmd = 'rm'
            elif act in ('hold', 'release'):
                subcmd = act
            else:
                raise SyntaxError("called with unknown action %s" % act)

            '''
            if test == true:
                os.open("echo jobsub_%s -G %s --role %s --jobid %s 2>&1" % (subcmd, group, cs.vo_role, ','.join(jjil)), "r")
            '''

            # expand launch setup %{whatever}s campaigns...

            launch_setup = lts.launch_setup

            launch_setup = launch_setup.strip()
            launch_setup = launch_setup.replace("\n", ";")
            launch_setup = launch_setup.strip(";")
            launch_setup = "source /grid/fermiapp/products/common/etc/setups;setup poms_client -g poms31 -z /grid/fermiapp/products/common/db;" + launch_setup

            cmd = """
                exec 2>&1
                export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s
                kinit -kt $HOME/private/keytabs/poms.keytab poms/cd/%s@FNAL.GOV || true
                ssh %s@%s '%s; set -x; jobsub_%s -G %s --role %s --jobid %s'
            """ % (
                group,
                self.poms_service.hostname,
                lts.launch_account,
                lts.launch_host,
                launch_setup,
                subcmd,
                group,
                cs.vo_role,
                ','.join(jjil)
            )

            cmd = cmd % {
                "dataset": cs.dataset,
                "version": cs.software_version,
                "group": group,
                "experimenter": st.experimenter_creator_obj.username,
                "experiment": cs.experiment,
            }

            f = os.popen(cmd, "r")
            output = f.read()
            f.close()

            return output, cs, campaign_stage_id, submission_id, job_id
        else:
            return "Nothing to %s!" % act, None, 0, 0, 0

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
