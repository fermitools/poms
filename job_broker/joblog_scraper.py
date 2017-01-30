#!/usr/bin/python

import sys
import os
import re
import urllib2
import json
import traceback
import time

from job_reporter import job_reporter

class joblog_scraper:
    def __init__(self,  job_reporter, debug = 0):
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
        self.copyin_re = re.compile(".*ifdh::cp\( (--force=[a-z]* )?(-D )?(/pnfs|/nova|/minerva|/grid|/cvmfs|/mu2e|/uboone|/lbne|/dune|/argoneut|/minos|/gm2|/miniboone|/coupp|/d0|/lariat|/e906|gsiftp:|s3:|http:)")
        self.job_id_map = {}
        self.job_task_map = {}

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

        # If jobs use old ifdh internally, but current ones in the
        # wrapper, we can use the host+experiment+pid to pick out
        # other messages we can attribute
        # so we don't grow forever, we start the mapping at "BEGIN ExE..."
        # and clean themou at "COMPLETED"...
        #
        # pick a key for this job -- hostname + pid
        key = hostname+experiment+pid

        # write down mapping at BEGIN EXECUTION
        if key and message.find("BEGIN EXECUTION") > 0 and jobsub_job_id and task:
            self.job_id_map[key] = jobsub_job_id
            self.job_task_map[jobsub_job_id] = task

        # clean up mapping at COMPLETED with...
        if key and message.find("COMPLETED with") > 0:
            if self.job_id_map.has_key(key):
                if self.job_task_map.has_key(self.job_id_map[key]):
                    del self.job_task_map[self.job_id_map[key]]
                del self.job_id_map[key]
  
        # use mapping to fill in missing bits
        if key and not jobsub_job_id and self.job_id_map.has_key(key):
            jobsub_job_id = self.job_id_map[key]

        if key and not task and jobsub_job_id and self.job_task_map.has_key(jobsub_job_id):
            task = self.job_task_map[jobsub_job_id]
            
        return { 
		'timestamp': timestamp.strip(),
		'hostname': hostname.strip(),
		'user': user.strip(),
		'experiment': experiment.strip(),
		'task': task.strip(),
		'jobsub_job_id': jobsub_job_id.strip(),
		'ifdh_vers': ifdh_vers.strip(),
		'pid': pid.strip(),
		'message': message.strip() ,
        }

    def find_files(self, message):
        if self.debug:
            print "looking for input/output files in: " , message
        file_map = {}
        message = message[message.find("ifdh::cp(")+9:]
        message = message[:message.find(")")]

        list = message.split(" ")
        for item in list:
            item = item[item.rfind("/")+1:]
            if item != "":
                file_map[item] = 1

        if self.debug:
            print "found files: " , file_map

        return ' '.join(file_map.keys())

    def report_item(self, taskid, jobsub_job_id, hostname, message, experiment = "none"):
        data = { 
           "taskid": taskid,
           "jobsub_job_id": jobsub_job_id,
           "node_name": hostname,
        }

        if self.debug:
           print "report_item: message:" , message

        if message.find("starting ifdh::cp") >= 0:
            if self.debug:
                print "saw copy"
	    if self.copyin_re.match(message):
                dir = "in"
                data['input_file_names'] = self.find_files(message)
            else:
                dir = "out"
                data['output_file_names'] = self.find_files(message)
             
            data['status'] = "running: copying files " + dir
        
        if message.find("transferred") > 0:
            # don't actually log copy completed status, can't tell where it is
            data['status'] = "running"
            pass

        if message.find("BEGIN EXECUTION") > 0:
           data['status'] = "running: user code"
           data['user_script' ] = message[message.find("EXECUTION")+11:]

        if message.find("COMPLETED with") > 0:
           if message[-1] == '0':
               data['status'] = "running: user code succeeded"
           else:
               data['status'] = "running: user code failed"
           data['user_exe_exit_code'] = message[message.find("COMPLETED with")+27:]
 
        # pull any fields if it's a json block
        pos = message.find('poms_data={')
        if pos >= 0:
           s = message[message.find('{'):]
           if self.debug: print "unpacking: " , s
           try:
              newdata = json.loads(s)
           except:
              s = s[0:s.find(', "bogo')] + " }"
              print "failed, unpacking: " , s
              try:
                  newdata = json.loads(s)
              except:
                  newdata = {}
                  print "still failed, continuing.."
                  pass

	   for k in newdata.keys():
	       if newdata[k] == '':
		   del newdata[k]

	   if newdata.has_key('vendor_id'):
               # round off bogomips so rounding errors do not give us
               # fake distinctions in bogomips
	       newdata['cpu_type'] = "%s@%s" % (
                       newdata['vendor_id'], str(round(float(newdata.get('bogomips','0')))))

	   data.update(newdata)

        if self.debug:
            print "reporting: " , data

        self.job_reporter.report_status(**data)


    def scan(self, filehandle):
        self.filehandle = filehandle
        for line in self.filehandle:
             #print "got: ", line
             d = self.parse_line(line)
             
             if d['task'] != '':
                 self.report_item(d['task'], d['jobsub_job_id'], d['hostname'],  d['message'])

if __name__ == '__main__':
   debug = 0
   if len(sys.argv) > 1 and sys.argv[1] == "-d":
        debug=1
        sys.argv = sys.argv[1:]

   server = "http://localhost:8080/poms"
   testing = False
   if len(sys.argv) > 1 and sys.argv[1] == "-t":
        print "doing test flag"
        server = "http://localhost:8888/poms"
        testing = True
        sys.argv = sys.argv[1:]

   js = joblog_scraper( job_reporter(server, debug=debug, namespace = "profiling.apps.poms.probes.joblog_scraper"), debug)
   while 1:
      if debug:
           print "Starting..."
      try:
          if testing:
              h = open(os.environ['TEST_JOBLOG'],"r")
          else:
              h = open("/home/poms/private/rsyslogd/joblog_fifo","r")
          if debug:
             print "re-reading...";

          js.scan(h)

          if testing:
              break

      except KeyboardInterrupt:
          break

      except:
          print time.asctime(), "Exception!"
          traceback.print_exc()
          pass
   js.job_reporter.cleanup()
