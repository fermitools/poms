#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from collections import deque
import re
from .poms_model import Job, Submission, CampaignStage, JobTypeSnapshot, JobFile, JobHistory
from datetime import datetime
from sqlalchemy.orm import joinedload
from sqlalchemy import func, not_, and_, or_, desc
from .utc import utc
import json
import os

from . import logit
from .pomscache import pomscache, pomscache_10


class JobsPOMS(object):

    pending_files_offset = 0

    def __init__(self, poms_service):
        self.poms_service = poms_service
        self.junkre = re.compile('.*fcl|log.*|.*\.log$|ana_hist\.root$|.*\.sh$|.*\.tar$|.*\.json$|[-_0-9]*$')

    def active_jobs(self, dbhandle):
        res = deque()
        for jobsub_job_id, submission_id in (dbhandle.query(Job.jobsub_job_id, Job.submission_id)
                                       .filter(Job.status != "Completed", Job.status != "Located", Job.status != "Removed", Job.status != "Failed")
                                       .execution_options(stream_results=True).all()):
            if jobsub_job_id == "unknown":
                continue
            res.append((jobsub_job_id, submission_id))
        logit.log("active_jobs: returning %s" % res)
        return res


    def output_pending_jobs(self, dbhandle):
        res = {}
        windowsize = 1000
        count = 0
        preve = None
        prevj = None
        # it would be really cool if we could push the pattern match all the
        # way down into the query:
        #  JobFile.file_name like JobTypeSnapshot.output_file_patterns
        # but with a comma separated list of them, I don's think it works
        # directly -- we would have to convert comma to pipe...
        # for now, I'm just going to make it a regexp and filter them here.
        for e, jobsub_job_id, fname in (dbhandle.query(
                                 CampaignStage.experiment,
                                 Job.jobsub_job_id,
                                 JobFile.file_name)
                  .join(Submission)
                  .filter(
                          Submission.status == "Completed",
                          Submission.campaign_stage_id == CampaignStage.campaign_stage_id,
                          Job.submission_id == Submission.submission_id,
                          Job.job_id == JobFile.job_id,
                          JobFile.file_type == 'output',
                          JobFile.declared == None,
                          Job.status == "Completed",
                        )
                  .order_by(CampaignStage.experiment, Job.jobsub_job_id)
                  .offset(JobsPOMS.pending_files_offset)
                  .limit(windowsize)
                  .all()):

            if preve != e:
                preve = e
                res[e] = {}
            if prevj != jobsub_job_id:
                prevj = jobsub_job_id
                res[e][jobsub_job_id] = []
            if not self.junkre.match(fname):
                logit.log("adding %s to exp %s jjid %s" % (fname, e, jobsub_job_id))
                res[e][jobsub_job_id].append(fname)
            count = count + 1

        if count != 0:
            JobsPOMS.pending_files_offset = JobsPOMS.pending_files_offset  + windowsize
        else:
            JobsPOMS.pending_files_offset = 0

        logit.log("pending files offset now: %d" % JobsPOMS.pending_files_offset)

        return res


    def update_SAM_project(self, samhandle, j, projname):
        logit.log("Entering update_SAM_project(%s)" % projname)
        sid = j.submission_obj.submission_id
        exp = j.submission_obj.campaign_stage_snapshot_obj.experiment
        cid = j.submission_obj.campaign_stage_snapshot_obj.campaign_stage_id
        samhandle.update_project_description(exp, projname, "POMS CampaignStage %s Submission %s" % (cid, sid))
        pass



    def kill_jobs(self, dbhandle, campaign_stage_id=None, submission_id=None, job_id=None, confirm=None, act='kill'):
        jjil = deque()
        jql = None
        s = None
        cs = None
        if campaign_stage_id is not None or submission_id is not None:
            if campaign_stage_id is not None:
                tl = dbhandle.query(Submission).filter(Submission.campaign_stage_id == campaign_stage_id,
                                                 Submission.status != 'Completed', Submission.status != 'Located', Submission.status != 'Failed').all()
            else:
                tl = dbhandle.query(Submission).filter(Submission.submission_id == submission_id).all()
            if len(tl):
                cs = tl[0].campaign_stage_snapshot_obj
                lts = tl[0].login_setup_snap_obj
                st = tl[0]
            else:
                cs = None
                lts = None


            for s in tl:
                tjid = s.jobsub_job_id
                logit.log("kill_jobs: submission_id %s -> tjid %s" % (s.submission_id, tjid))
                # for submissions/campaign_stages, kill the whole group of jobs
                # by getting the leader's jobsub_job_id and taking off
                # the '.0'.
                if tjid:
                    jjil.append(tjid.replace('.0', ''))
        else:
            jql = dbhandle.query(Job).filter(Job.job_id == job_id,
                                             Job.status != 'Completed', Job.status != 'Removed',
                                             Job.status != 'Located', Job.status != 'Failed').execution_options(stream_results=True).all()

            if len(jql) == 0:
                jjil = ["(None Found)"]
            else:
                st = jql[0].submission_obj
                cs = st.campaign_stage_snapshot_obj
                for j in jql:
                    jjil.append(j.jobsub_job_id)
                lts = st.login_setup_snap_obj

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

            launch_setup = launch_setup.replace("\n",";")
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
                "experimenter":  st.experimenter_creator_obj.username,
                "experiment": cs.experiment,
                }

            f = os.popen(cmd, "r")
            output = f.read()
            f.close()

            return output, cs, campaign_stage_id, submission_id, job_id
        else:
            return "Nothing to %s!" % act, None, 0, 0, 0



    def get_efficiency(self, dbhandle, id_list, tmin, tmax):  #This method was deleted from the main script

        if isinstance(id_list, str):
            id_list = [int(cid) for cid in id_list.split(',') if cid]

        mapem = self.get_efficiency_map(dbhandle, id_list, tmin, tmax)
        efflist = deque()
        for cid in id_list:
            efflist.append(mapem.get(cid, -2))

        logit.log("got list: %s" % repr(efflist))
        return efflist
