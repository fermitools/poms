#!/usr/bin/env python

import sys
import os
import re
import urllib2
import json

class job_reporter:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, report_url):
        self.report_url = report_url

    def report_status(self, jobid, taskid, jobstatus, **kwargs):
        uh = urllib2.urlopen(self.report_url + "/report_job_status", data = data)
        res = uh.read()
        uh.close()
        return res
