#!/usr/bin/env python

import sys
import os
import re

class joblog_scraper:
    def __init__(self):
        # lots of names for parts of regexps to make it readable(?)
        timestamp_pat ="[-0-9T:]*"
        hostname_pat="[a-z0-9.]*"
        idpart_pat"[^[/:]*"
        user_pat=idpart_pat
        jobid_pat=idpart_pat
        taskid_pat=idpart_pat
        exp_pat=idpart_pat
        ifdh_vers_pat = idpart_pat
        pid_pat = "[0-9]*]"
        ifdhline_pat = "%s %s %s/%s:? ?%s/%s/%s/%s\[%s\] ifdh: (.*)" % (
             timestamp_pat, hostname_pat, user_pat, exp_pat, taskid_pat,
             jobid_pat, ifdh_vers_pat, exp_pat, pid_pat) 
        oldifdhline_pat = "%s %s %s/%s/%s\[%s\] ifdh: (.*)" % (
	     timestamp_pat, hostname_pat, exp_pat, ifdh_vers_pat, exp_pat, 
             pid_pat)
        self.ifdhline_re = re.compile(ifdhline_pat)
        self.oldifdhline_re = re.compile(oldifdhline_pat)

    def parse_line(self, line):
	timestamp = ""
	hostname = ""
	user = ""
	experiment = ""
	task = ""
	jobid = ""
	ifdh_vers = ""
	experiment = ""
	pid = ""
	message  = ""
        m1 = self.olifdhline.match(line)
        m2 = self.ifdhline.match(line)
        if m1:
            timestamp, hosntame, experiment, ifdh_vers, experiment, pid, message = m.groups()
            user = experiment
        elif m2:
            timestamp, hosntame, user, experiment, task, jobid, ifdh_vers, experiment, pid, message = m.groups()
        else:
            message = line
        return { 
		'timestamp': timestamp,
		'hostname': hostname,
		'user': user,
		'experiment': experiment,
		'task': task,
		'jobid': jobid,
		'ifdh_vers': ifdh_vers,
		'experiment': experiment,
		'pid': pid,
		'message ': message ,
        }

    def report_item(self, taskid, jobid, hostname, message):
        pass

    def scan(self, file):
        for line in file:
             d = self.parse_line(line)
             if d['task'] != '':
                 self.report_item(d['task'], d['jobid'], d['hostname'],  d['message'],
     

if __name__ == '__main__':
    js = joblog_scraper()
    js.scan(sys.stdin)
