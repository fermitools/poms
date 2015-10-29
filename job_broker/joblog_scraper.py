#!/usr/bin/python

import sys
import os
import re
import urllib2
import json
import traceback

from job_reporter import job_reporter

class joblog_scraper:
    def __init__(self, filehandle, job_reporter, debug = 0):
        self.filehandle = filehandle
        self.job_reporter = job_reporter
        self.debug = debug

        # lots of names for parts of regexps to make it readable(?)
        timestamp_pat ="[-0-9T:]*"
        hostname_pat="[-A-Za-z0-9.]*"
        idpart_pat="[^[/:]*"
        user_pat=idpart_pat
        jobsub_job_id_pat=idpart_pat
        taskid_pat=idpart_pat
        exp_pat=idpart_pat
        ifdh_vers_pat = idpart_pat
        pid_pat = "[0-9]*"

        ifdhline_pat = "(%s) (%s) (%s)/(%s):? ?(%s)/(%s)/(%s)/(%s)\[(%s)\]:.ifdh:(.*)" % (
		timestamp_pat, hostname_pat, user_pat, exp_pat,taskid_pat,
                jobsub_job_id_pat, ifdh_vers_pat,exp_pat, pid_pat )

        oldifdhline_pat ="(%s) (%s) (%s)/(%s)/(%s)\[(%s)\]:.ifdh:(.*)" % (
	     timestamp_pat, hostname_pat, exp_pat, ifdh_vers_pat, exp_pat, 
             pid_pat)

        self.ifdhline_re = re.compile(ifdhline_pat)
        self.oldifdhline_re = re.compile(oldifdhline_pat)
        self.copyin_re = re.compile(".*ifdh::cp\( (--force=[a-z]* )?(-D )?(/pnfs|/nova|/minerva|/grid|/cvmfs|/mu2e|/uboone|/lbne|/dune|/argoneut|/minos|/gm2|/miniboone|/coupp|/d0|/lariat|/e906|gsiftp:|s3:)")

    def parse_line(self, line):
	timestamp = ""
	hostname = ""
	user = ""
	experiment = ""
	task = ""
	jobsub_job_id = ""
	ifdh_vers = ""
	experiment = ""
	pid = ""
	message  = ""
        m1 = self.oldifdhline_re.match(line)
        m2 = self.ifdhline_re.match(line)
        if m1:
            timestamp, hostname, experiment,  ifdh_vers, experiment, pid, message = m1.groups()
            user = experiment
        elif m2:
            timestamp, hostname, user, experiment, task, jobsub_job_id, ifdh_vers, experiment, pid, message = m2.groups()
        else:
            message = line
        return { 
		'timestamp': timestamp,
		'hostname': hostname,
		'user': user,
		'experiment': experiment,
		'task': task,
		'jobsub_job_id': jobsub_job_id,
		'ifdh_vers': ifdh_vers,
		'pid': pid,
		'message': message ,
        }

    def find_output_files(self, message):
        if self.debug:
            print "looking for output files in: " , message
        file_map = {}
        message = message[message.find("ifdh::cp(")+9:]
        list = message.split(" ")
        for item in list:
            item = item[item.rfind("/")+1:]
            # pretty much all actual output files are .root or .art ...
            if item.endswith(".root") or item.endswith(".art"):
               file_map[item] = 1

        if self.debug:
            print "found files: " , file_map

        return ' '.join(file_map.keys())

    def report_item(self, taskid, jobsub_job_id, hostname, message, experiment = "none"):
        data = { 
           "taskid": taskid,
           "jobsub_job_id": jobsub_job_id,
           "slot": hostname,
        }

        if self.debug:
           print "report_item: message:" , message

        if message.find("starting ifdh::cp") > 0:
            if self.debug:
                print "saw copy"
	    if self.copyin_re.match(message):
                dir = "in"
            else:
                dir = "out"

            if dir == "out":
                data['output_file_names'] = self.find_output_files(message)
             
            data['status'] = "running: copying files " + dir
        
        if message.find("transferred") > 0:
            # don't actually log copy completed status, can't tell where it is
            # data['status'] = "running: copy succeeded"
            pass

        if message.find("BEGIN EXECUTION") > 0:
           data['status'] = "running: user code"
           data['user_script' ] = message[message.find("EXECUTION")+11:]

        if message.find("COMPLETED with") > 0:
           data['status'] = "running: user code complete"
           data['user_exe_exit_code'] = message[message.find("COMPLETED with")+27:]
 
        # pull any fields if it's a json block
        pos = message.find('{')
        if pos > 0 and pos < 4:
           s = message[message.find('{'):]
           print "unpacking: " , s
           try:
              data.update(json.loads(s))
           except:
              s = s[0:s.find(', "bogo')] + " }"
              print "failed, unpacking: " , s
              try:
                  data.update(json.loads(s))
              except:
                  print "still failed, continuing.."
                  pass

        if self.debug:
            print "reporting: " , data

        self.job_reporter.report_status(**data)


    def scan(self):
        for line in self.filehandle:
             d = self.parse_line(line)
             if d['task'] != '':
                 self.report_item(d['task'], d['jobsub_job_id'], d['hostname'],  d['message'])

if __name__ == '__main__':
   debug = 0
   if len(sys.argv) > 1 and sys.argv[1] == "-d":
        debug=1

   while 1:
      try:
          h = open("/fife/local/data/ifmon/joblog_fifo","r")
          # for testing
          #h = open("/tmp/mengel_jobs","r")
          if debug:
             print "re-reading...";

          js = joblog_scraper(h, job_reporter("http://fermicloud045.fnal.gov:8080/poms/", debug), debug)
          js.scan()
          # for testing
          #break

      except KeyboardInterrupt:
          break

      except:
          print "Exception!"
          traceback.print_exc()
          pass
