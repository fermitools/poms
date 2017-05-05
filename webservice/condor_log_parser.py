#!/usr/bin/env python

# our own logging handle, goes to cherrypy
import logging
logger = logging.getLogger('cherrypy.error')

from . import jobsub_fetcher
from datetime import datetime, timedelta
from poms.model.poms_model import Job

def get_joblogs(dbhandle, jobsub_job_id, experiment, role):
    if jobsub_job_id == None:
        return
    jf = jobsub_fetcher.jobsub_fetcher()
    logger.debug( "checking index" )
    files = jf.index( jobsub_job_id, experiment, role, True)
    for row in files:
        if row[5].endswith(".log") and not row[5].endswith(".dagman.log"):
            # first non dagman.log .log we see is either the xyz.log 
            # that goes with xyz.cmd, or the dag.nodes.log, which has
            # the individual job.logs in it.
            logger.debug( "checking file %s " %row[5] )
            lines = jf.contents(row[5], jobsub_job_id, experiment, role)
            parse_condor_log(dbhandle, lines, jobsub_job_id[jobsub_job_id.find("@")+1:])
            break
    del jf

def fix_jobid(clust_proc, batchhost):
    ''' convert 123456.010.000 to 123456.10@batchost '''
    p1 = clust_proc.find('.')
    p2 = clust_proc.find('.',p1+1)
    cluster = clust_proc[0:p1]
    proc = int(clust_proc[p1+1:p2])
    return "%s.%d@%s" % ( cluster, proc, batchhost )

def compute_secs(time_str):
    ''' convert hh:mm:ss to seconds '''
    time_str = time_str.strip(",")
    tl = [int(x) for x in time_str.split(":")]
    return (tl[0] * 60 + tl[1] ) *60 + tl[2]

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
    ty, tm = datetime.now().strftime("%Y %m").split()
    lmy, lm = (datetime.now() - timedelta(days=27)).strftime("%Y %m").split()

    if date_time_str[:2] == tm:
       date_time_str = "%s/%s" % (ty , date_time_str)
    elif date_time_str[:2] == lm:
       date_time_str = "%s/%s" % (lmy , date_time_str)
    else:
       # if it is some other month, just guess this year.. sorry
       date_time_str = "%s/%s" % (ty , date_time_str)
     
    return datetime.strptime(date_time_str, "%Y/%m/%d %H:%M:%S")

def parse_condor_log(dbhandle, lines, batchhost):
    in_termination = 0
    stimes = {}
    for line in lines:
        if line[:5] == "001 (":
            logger.debug( "start record start: %s" % line )
            ppos = line.find(")")
            jobsub_job_id = fix_jobid(line[5:ppos], batchhost)
            stimes[jobsub_job_id] = parse_date(line[ppos+2:ppos+16])
        if line[:5] == "005 (":
            logger.debug( "term record start: %s" % line )
            ppos = line.find(")")
            in_termination = 1
            finish_time = parse_date(line[ppos+2:ppos+16])
            jobsub_job_id = fix_jobid(line[5:ppos], batchhost)
            remote_cpu = None
            disk_used = None
            memory_used = None
            continue
        if line[:3] == "..." and in_termination:
            logger.debug( "term record end %s" % line )
            job = dbhandle.query(Job).with_for_update().filter(Job.jobsub_job_id == jobsub_job_id).first()
            if job:
                job.cpu_time = remote_cpu
                job.wall_time = (finish_time - stimes[jobsub_job_id]).total_seconds()
                logger.debug( "start: %s end: %s wall_time %s "%( stimes[jobsub_job_id],  finish_time,  job.wall_time ))
            else:
                # XXX we should create the job 'cause jobsub_q agent, etc missed it...
                pass

            in_termination = 0
            continue
        if in_termination:
            logger.debug( "saw: ", line )
            if line.find("(return value") > 0:
                 job_exit = int(line.split()[5].strip(')'))
            if line.find("Total Remote Usage") > 0:
                 remote_cpu = compute_secs(line.split()[2])
            if line.find("Disk (KB)") > 0:
                 disk_used = line.split()[3]
            if line.find("Memory (KB)") > 0:
                 memory_used = line.split()[3]
            logger.info( "condor_log_parser: remote_cpu %s disk_used %s memory_used %s job_exit %s" % (remote_cpu,  disk_used,  memory_used, job_exit ))

    dbhandle.commit()

