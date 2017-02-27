#!/usr/bin/env python


### This module contain the methods that handle the Calendar. (5 methods)
### List of methods: handle_dates, quick_search, jump_to_job, test_job_counts, task_min_job
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.
from poms.model.poms_model import  Job
from datetime import datetime, tzinfo,timedelta
from utc import utc
import urllib



class UtilsPOMS():
    def __init__(self, ps):
        self.poms_service = ps

    def handle_dates(self, tmin, tmax, tdays, baseurl): #this method was deleted from the main script
        """
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,tranges = self.handle_dates(tmax, tdays, name)
        assuming tmin, tmax, are date strings or None, and tdays is
        an integer width in days, come up with real datetimes for
        tmin, tmax, and string versions, and next ane previous links
        and a string describing the date range.  Use everywhere.
        """

        # set a flag to remind us to set tdays from max and min if
        # they are both set coming in.
        set_tdays =  (tmax != None and tmax != '') and (tmin != None and tmin!= '')

        if tmax == None or tmax == '':
            if tmin != None and tmin != '' and tdays != None and tdays != '':
                if isinstance(tmin, basestring):
                    tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)
                tmax = tmin + timedelta(days=float(tdays))
            else:
                # if we're not given a max, pick now
                tmax = datetime.now(utc)

        elif isinstance(tmax, basestring):
            tmax = datetime.strptime(tmax[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)

        if tdays == None or tdays == '':  # default to one day
            tdays = 1

        tdays = float(tdays)

        if tmin == None or tmin == '':
            tmin = tmax - timedelta(days = tdays)

        elif isinstance(tmin, basestring):
            tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)

        if set_tdays:
            # if we're given tmax and tmin, compute tdays
            tdays = (tmax - tmin).total_seconds() / 86400.0

        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days = tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmaxs =  tmax.strftime("%Y-%m-%d %H:%M:%S")
        tmins =  tmin.strftime("%Y-%m-%d %H:%M:%S")
        prevlink="%s/%stmax=%s&tdays=%d" % (self.poms_service.path,baseurl,tsprev, tdays)
        nextlink="%s/%stmax=%s&tdays=%d" % (self.poms_service.path,baseurl,tsnext, tdays)
        # if we want to handle hours / weeks nicely, we should do
        # it here.
        plural =  's' if tdays > 1.0 else ''
        tranges = '%6.1f day%s ending <span class="tmax">%s</span>' % (tdays, plural, tmaxs)

        # redundant, but trying to rule out tz woes here...
        tmin = tmin.replace(tzinfo = utc)
        tmax = tmax.replace(tzinfo = utc)


        return tmin,tmax,tmins,tmaxs,nextlink,prevlink,tranges


    def quick_search(self, dbhandle, redirect, search_term):
        search_term = search_term.strip()
        job_info = dbhandle.query(Job).filter(Job.jobsub_job_id == search_term).first()
        if job_info:
            tmins =  datetime.now(utc).strftime("%Y-%m-%d+%H:%M:%S")
            raise redirect("%s/triage_job?job_id=%s&tmin=%s" % (self.poms_service.path,str(job_info.job_id),tmins))
        else:
            search_term = search_term.replace("+", " ")
            query = urllib.urlencode({'q' : search_term})
            raise redirect("%s/search_tags?%s" % (self.poms_service.path, query))
