#!/usr/bin/env python

def get_joblogs(dbhandle, jobsub_job_id, experiment, role):
    jf = jobsub_fetcher()
    files = jf.index( jobsub_job_id, experiment, role, True)
    for row in files:
        if row[5].endswith(".log") and not row[5].endswith(".dagman.log"):
            # first non dagman.log .log we see is either the xyz.log 
            # that goes with xyz.cmd, or the dag.nodes.log, which has
            # the individual job.logs in it.
            lines = jf.contents(jobsub_job_id, experiment, role, row[5])
            parse_condor_log(dbhandle, lines, jobsub_job_id[jobsub_job_id.find("@")+1:])
            break
    del jf

def fix_jobid(clust_proc, batchhost):
    ''' convert 123456.010.000 to 123456.10@batchost '''
    p1 = clust_proc.find('.')
    p2 = clust_proc.find('.',p1+1)
    cluster = clust_proc[0:p1]
    proc = int(clust_proc[p1+1:p2]
    return "%s.%d@%s" % ( cluster, proc, batchhost )

def compute_secs(time_str):
    ''' convert hh:mm:ss to seconds '''
    tl = [int(x) for x in time_str.split(":")]
    return (tl[0] * 60 + tl[1] ) *60 + tl[2]

def parse_condor_log(dbhandle, lines, batchhost):
    for line in lines:
        if line[:5] == "005 (":
            ppos = line.find(")")
            in_termination = 1
            finish_time = datetime.striptime("%m/%d %H:%M:%S",line[ppos+12:])
            jobsub_job_id = fix_jobid(line[5:ppos], batchhost)
            remote_cpu = None
            disk_used = None
            memory_used = None
            continue
        if line[:3] == "..." and in_termination:
            job = dbhandle.query(Job).with_for_update().filter(jobsub_jobid == jobsub_jobid).first()
            job.cpu_time = remote_cpu
            in_termination = 0
            continue
        if in_termination:
            if line.find("Total Remote Usage") > 0:
                 remote_cpu = compute_secs(line.split()[2])
            if line.find("Disk (KB)") > 0:
                 disk_used = line.split()[3]
            if line.find("Memory (KB)") > 0:
                 disk_used = line.split()[3]

    dbhandle.commit()
