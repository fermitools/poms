#!/usr/bin/env python

### This module contain the methods that handle the
### List of methods: def list_task_logged_files, campaign_task_files, job_file_list
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.

def job_counts(self, task_id = None, campaign_id = None, tmin = None, tmax = None, tdays = None): ###IM HERE
    tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.handle_dates(tmin, tmax,tdays,'job_counts')

    q = dbhandle.query(func.count(Job.status),Job.status). group_by(Job.status)
    if tmax != None:
        q = q.filter(Job.updated <= tmax, Job.updated >= tmin)

    if task_id:
        q = q.filter(Job.task_id == task_id)

    if campaign_id:
        q = q.join(Task,Job.task_id == Task.task_id).filter( Task.campaign_id == campaign_id)

    out = OrderedDict([("Idle",0),( "Running",0),( "Held",0),( "Completed",0), ("Located",0),("Removed",0)])
    for row in  q.all():
        # this rather bizzare hoseyness is because we want
        # "Running" to also match "running: copying files in", etc.
        # so we ignore the first character and do a match
        if row[1][1:7] == "unning":
            short = "Running"
        else:
            short = row[1]
        out[short] = out.get(short,0) + int(row[0])

    return out
