#!/usr/bin/env python

import sys
import os
import re
import requests
import json
import time
import traceback
import resource
import cherrypy
import gc
import pprint
from job_reporter import job_reporter
sys.path.append("../webservice")
from elasticsearch import Elasticsearch

# don't barf if we need to log utf8...
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

class jobsub_es_scraper:
    """
        Pull info from ElasticSearch to update job status in POMS database
    """
    def __init__(self, job_reporter, debug = 0):
        self.rs = requests.Session()
        self.job_reporter = job_reporter
        self.map = {
           "0": "Unexplained",
           "1": "Idle",
           "2": "Running",
           "3": "Removed",
           "4": "Completed",
           "5": "Held",
           "6": "Submission_error",
        }
        self.cur_report = {}
        self.prev_report = {}
        self.debug = debug
        self.passcount = 0
        self.es = Elasticsearch(config=cherrypy.config)
        self.ThisRun = time.time()
        self.LastRun = self.ThisRun 
        self.FirstRun = True
        self.FinishedJobs = {}
        self.ActiveJobs = []

    def getAllPomsActive(self):
        try:
            conn = self.rs.get(self.job_reporter.report_url + '/active_jobs')
            self.ActiveJobs = conn.json()
            conn.close()
            del conn
            conn = None

            print "got %d active POMS jobs" % len(self.ActiveJobs)

	except KeyboardInterrupt:
	    raise
        except:
            print  "Ouch!", sys.exc_info()
            traceback.print_exc()
            if conn: del conn

    def scan(self):

        self.FinishedJobs = {}
        # Lines could take minutes to show up in elasticsearch so go back 300 seconds
        # before the end time of the last run to catch them.  This will cause duplicates
        # but we will just marked stuff completed again.
        self.LastRun = self.ThisRun - 300
        self.ThisRun = time.time()
        if ( self.FirstRun == True):
            # First run of script, catching up by checking all POMS jobs
            self.FirstRun = False
            self.getAllPomsActive()
            self.checkActive()
        else:
            self.getRecent()

        self.UpdateJobs()

    def checkActive(self):
        # check all active jobs in ES and update ones that have ended
        # if TerminatedEvent exists add to self.FinishedJobs
        print ("CATCHUP FUNCTION NOT IMPLEMENTED YET")
        print self.ActiveJobs

    def UpdateJobs(self):

        for jid in self.FinishedJobs:
            args = self.FinishedJobs[jid]
            try:
                print self.FinishedJobs[jid]
                self.job_reporter.report_status(**args)
            except KeyboardInterrupt:
                raise
            except:
                print "Reporting Exception!"
                traceback.print_exc()
                pass


    def getRecent(self):
        EndTime=time.strftime("%Y-%m-%dT%X",time.localtime(self.ThisRun))
        StartTime=time.strftime("%Y-%m-%dT%X",time.localtime(self.LastRun))
        query = {
            "size" : 500,
            "query": {
                "bool": {
                    "filter": [
                        {"range": {
                            "EventTime": {
                                "lte": EndTime,
                                "gte": StartTime
                            }
                        }},
                        {"term": {"MyType": "JobTerminatedEvent"}},
                        {"exists": {"field": "POMS_TASK_ID"}},
                    ]
                }
            }
        }

        response = self.es.search(index='fifebatch-logs-*', query=query)

        print("%s POMS jobs ended this run: %d" % (time.asctime(),response['hits']['total']))

        for record in response['hits']['hits']:
            jid = record.get('_source').get('jobid')
            self.FinishedJobs[jid] = {}

            # Do not have in event_log: task_recovery_tasks_parent,wall_time, task_project,node_name,restarts

            self.FinishedJobs[jid] = {
                "cpu_time" : int(record.get('_source').get('run_remote_sys_time')) + int(record.get('_source').get('run_remote_user_time')),
                "host_site" : record.get('_source').get('MachineAttrGLIDEIN_Site0'),
                "status" : "Completed",
                "taskid" : record.get('_source').get('POMS_TASK_ID'),
                "jobsub_job_id" : jid
            }


        # Maybe someone does a big condor_rm and lots go away at once
        if (len(self.FinishedJobs) < response['hits']['total']):
            sys.stderr.write("ERROR, OVER SIZE LIMIT %d %d\n" % (len(self.FinishedJobs),response['hits']['total']))

    def poll(self):
        while(1):
            self.passcount = self.passcount + 1

            # just restart periodically, so we don't eat memory, etc.
            #if self.passcount > 1000:
            #    os.execvp(sys.argv[0], sys.argv)

            try:
                self.scan()
			 
	    except KeyboardInterrupt:
	        raise
 
            except OSError as e:
	        print "Exception!"
	        traceback.print_exc()
                # if we're out of memory, dump core...
                if e.errno == 12:
                    resource.setrlimit(resource.RLIMIT_CORE,resource.RLIM_INFINITY)
                    os.abort()

	    except:
	        print "Exception!"
	        traceback.print_exc()
	        pass

            time.sleep(30)

if __name__ == '__main__':
    debug = 0
    if len(sys.argv) > 1 and sys.argv[1] == "-d":
        debug=1

    js = jobsub_es_scraper(job_reporter("http://localhost:8080/poms", debug=debug), debug = debug)
    #js = jobsub_es_scraper(job_reporter("http://pomsgpvm01.fnal.gov:8080/poms", debug=debug), debug = debug)
    try:
        js.poll()
    except KeyboardInterrupt:
        print "Exiting from keyboard interrupt"
        js.job_reporter.cleanup()
    
