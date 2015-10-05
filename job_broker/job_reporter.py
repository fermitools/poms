#!/usr/bin/env python

import sys
import os
import re
import urllib2
import urllib
import json

class job_reporter:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, report_url):
        self.report_url = report_url

    def report_status(self, jobid = '', taskid = '', status = '' , cpu_type = '', slot='', **kwargs ):
        data = {}
        data['task_id'] = taskid
        data['jobsubjobid'] = jobid
        data['cpu_type'] = cpu_type
        data['slot'] = slot
        data['status'] = status
          
        uh = urllib2.urlopen(self.report_url + "/update_job", data = urllib.urlencode(data))
        res = uh.read()
        uh.close()
        return res
