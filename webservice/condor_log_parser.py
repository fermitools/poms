#!/usr/bin/env python

"""
Parser for condor job log files to get information out
"""

from datetime import datetime, timedelta

from .logit import log
from . import jobsub_fetcher
from .poms_model import Submission


# our own logging handle, goes to cherrypy
def get_joblogs(dbhandle, jobsub_job_id, cert, key, experiment, role):
    '''
        get the condor joblog for a given job
    '''
    log("INFO", "entering get_joblogs")
    if jobsub_job_id is None:
        return
    fetcher = jobsub_fetcher.jobsub_fetcher(cert, key)
    log("DEBUG", "checking index")
    files = fetcher.index(jobsub_job_id, experiment, role, True)
    task = dbhandle.query(Submission.submission_id).filter(
        Submission.jobsub_job_id == jobsub_job_id).first()
    if task is None:
        submission_id = 14
    else:
        submission_id = task.submission_id

    if files is None:
        return

    for row in files:
        if row[5].endswith(".log") and not row[5].endswith(".dagman.log"):
            # first non dagman.log .log we see is either the xyz.log
            # that goes with xyz.cmd, or the dag.nodes.log, which has
            # the individual job.logs in it.
            log("DEBUG", "checking file %s " % row[5])
            lines = fetcher.contents(row[5], jobsub_job_id, experiment, role)
            parse_condor_log(dbhandle,
                             lines,
                             jobsub_job_id[jobsub_job_id.find("@") + 1:],
                             submission_id)
            break
    del fetcher


def fix_jobid(clust_proc, batchhost):
    ''' convert 123456.010.000 to 123456.10@batchost '''
    pos1 = clust_proc.find('.')
    pos2 = clust_proc.find('.', pos1 + 1)
    cluster = clust_proc[0:pos1]
    proc = int(clust_proc[pos1 + 1:pos2])
    return "%s.%d@%s" % (cluster, proc, batchhost)


def compute_secs(time_str):
    ''' convert hh:mm:ss to seconds '''
    time_str = time_str.strip(",")
    timelist = [int(x) for x in time_str.split(":")]
    return (timelist[0] * 60 + timelist[1]) * 60 + timelist[2]


def parse_date(date_time_str):
    ''' condor just gives month/day, so add the year and parse
        -- the trick is to add the *right* year.  At the year boundary
           (i.e. it's Jan 1, and the job started on Dec 31) we may
           need to pick *yesterday's* year, not todays... so check
           by checking yesterdays month.
           ... in fact we should go a little further back (27 days)
           for to get last month right further into this month.
    '''
    # get todays, yesterdays year and month
    t_year, t_month = datetime.now().strftime("%Y %m").split()
    lm_year, lm_month = (datetime.now() - timedelta(days=27)).strftime("%Y %m").split()

    if date_time_str[:2] == t_month:
        date_time_str = "%s/%s" % (t_year, date_time_str)
    elif date_time_str[:2] == lm_month:
        date_time_str = "%s/%s" % (lm_year, date_time_str)
    else:
        # if it is some other month, just guess this year.. sorry
        date_time_str = "%s/%s" % (t_year, date_time_str)

    return datetime.strptime(date_time_str, "%Y/%m/%d %H:%M:%S")


def parse_condor_log(dbhandle, lines, batchhost, submission_id):
    ''' read a condor log looking for start/end info '''
    in_termination = 0
    stimes = {}
    job_sites = {}
    execute_hosts = {}
    job_exit = None
    jobsub_job_id = None
    for line in lines:
        if line[:5] == "001 (":
            log("DEBUG", "start record start: %s" % line)
            ppos = line.find(")")
            jobsub_job_id = fix_jobid(line[5:ppos], batchhost)
            stimes[jobsub_job_id] = parse_date(line[ppos + 2:ppos + 16])
        if line[:5] == "001 (":
            log("DEBUG", "job classad record start: %s" % line)
            jobsub_job_id = fix_jobid(line[5:ppos], batchhost)

        if line[:10] == "JOB_Site =":
            job_sites[jobsub_job_id] = line[11:-1]

        if line[:13] == "ExecuteHost =":
            execute_hosts[jobsub_job_id] = line[15:-2]

        if line[:5] == "005 (":
            log("DEBUG", "term record start: %s" % line)
            ppos = line.find(")")
            in_termination = 1
            finish_time = parse_date(line[ppos + 2:ppos + 16])
            jobsub_job_id = fix_jobid(line[5:ppos], batchhost)
            remote_cpu = None
            disk_used = None
            memory_used = None
            continue
        if line[:3] == "..." and in_termination:
            log("DEBUG", "term record end %s" % line)
            submission = dbhandle.query(Submission).with_for_update(
                read=True).filter(Submission.jobsub_job_id == jobsub_job_id).first()

            in_termination = 0
            continue
        if in_termination:
            log("DEBUG", "saw: ", line)
            if line.find("termination (signal ") > 0:
                job_exit = 128 + int(line.split()[5].strip(')'))
            if line.find("termination (return value") > 0:
                job_exit = int(line.split()[5].strip(')'))
            if line.find("Total Remote Usage") > 0:
                remote_cpu = compute_secs(line.split()[2])
            if line.find("Disk (KB)") > 0:
                disk_used = line.split()[3]
            if line.find("Memory (KB)") > 0:
                memory_used = line.split()[3]
            log("INFO", "condor_log_parser: remote_cpu %s "
                "disk_used %s memory_used %s job_exit %s" % (
                    remote_cpu, disk_used, memory_used, job_exit))

    dbhandle.commit()
