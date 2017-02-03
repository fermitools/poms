#!/usr/bin/env python

import sys
import os
import re
import urllib2
import urllib
import httplib
import json
import concurrent.futures
import Queue
import thread
import threading
import time
from prometheus_client.bridge.graphite import GraphiteBridge

class job_reporter:
    """
       class to report job status -- now runs several threads to asynchronously report queued items
       given to us, so we don't drop messages from syslog piping to us, etc.
    """
    def __init__(self, report_url, debug = 0, nthreads = 5, namespace = ""):
        self.namespace = namespace
        self.gb = GraphiteBridge(('fermicloud079.fnal.gov', 2003))
        self.gb.start(10,prefix=self.namespace)
        self.report_url = report_url
        self.debug = debug
        self.work = Queue.Queue()
        self.wthreads = []
        self.nthreads = nthreads
        for i in range(nthreads):
            self.wthreads.append(threading.Thread(target=self.runqueue))
            #self.wthreads[i].daemon = True
            self.wthreads[i].start()

    def check(self):
        # make sure we still have nthreads reporting threads
        for i in range(self.nthreads):
            if not(self.wthreads[i].isAlive()):
                self.wthreads[i].join(0.1)
                self.wthreads[i] = threading.Thread(target=self.runqueue) 
                self.wthreads[i].start()

    def bail(self):
        print "thread: %d -- bailing" % thread.get_ident()
        raise KeyboardInterrupt("just quitting a thread")

    def runqueue(self):
	while 1:
            try:
                d = self.work.get(block = True)
                d['f'](*d['args'], **d['kwargs'])
            except KeyboardInterrupt:
                break
            except:
                print "Unhandled exception", sys.exc_info()
                time.sleep(1)
                pass
  
    def cleanup(self):
        # first tell threads to exit
        for wth in self.wthreads:
            self.work.put({'f': job_reporter.bail, 'args': [self], 'kwargs':{}})

        # then wait for them
        for wth in self.wthreads:
            wth.join()

    def report_status(self,  jobsub_job_id = '', taskid = '', status = '' , cpu_type = '', slot='', **kwargs ):
        self.check()
        self.work.put({'f':job_reporter.actually_report_status, 'args': [ self, jobsub_job_id, taskid, status, cpu_type, slot], 'kwargs': kwargs})

    def actually_report_status(self, jobsub_job_id = '', taskid = '', status = '' , cpu_type = '', slot='', **kwargs ):
        data = {}
        data['task_id'] = taskid
        data['jobsub_job_id'] = jobsub_job_id
        data['cpu_type'] = cpu_type
        data['slot'] = slot
        data['status'] = status
        data.update(kwargs)

        if self.debug:
           sys.stdout.write("%s: reporting: %s\n" %  (time.asctime(), data) )
           sys.stdout.flush()

        retries = 3
          
        uh = None
        res = None
      
        while retries > 0:
	    try:
		uh = urllib2.urlopen(self.report_url + "/update_job", data = urllib.urlencode(data))
		res = uh.read()
                uh.close()
		if self.debug: sys.stderr.write("response: %s\n" % res)

                del uh
                uh = None

		return res


	    except (urllib2.HTTPError) as e:
		sys.stderr.write("Exception: HTTP error %d" % e.code)
		sys.stderr.write("\n--------\n")
                sys.stderr.flush()

                if uh:
                    uh.read()
                    uh.close()
                    del uh
                    uh = None

                # don't retry on 401's...
                if e.code in [401,404]:
                    del e
                    return ""

		del e
                time.sleep(5)
                retries = retries - 1

	    except (urllib2.URLError) as e:
                if uh:
                    uh.read()
                    uh.close()
                    del uh
                    uh = None
		errtext = str(e)
		sys.stderr.write("Exception:" + errtext)
		sys.stderr.write("\n--------\n")
                sys.stderr.flush()
                del e
                time.sleep(5)
                retries = retries - 1
	    except (httplib.BadStatusLine) as e:
                if uh:
                    uh.read()
                    uh.close()
                    del uh
                    uh = None
		errtext = str(e)
		sys.stderr.write("Exception:" + errtext)
		sys.stderr.write("\n--------\n")
                sys.stderr.flush()
                del e
                time.sleep(5)
                retries = retries - 1
                
	    except (KeyboardInterrupt):
                raise

	    except (Exception) as e:
		errtext = str(e)
		sys.stderr.write("Unknown Exception:" + errtext + sys.exc_info())
		sys.stderr.write("\n--------\n")
                sys.stderr.flush()
                raise

if __name__ == '__main__':
    print "self test:"
    r = job_reporter("http://127.0.0.1:8080/poms", debug=1)
    r.report_status(jobsub_job_id="12345.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12346.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12347.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12348.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    print "started reports:"
    sys.stdout.flush()
    r.cleanup()
    r = job_reporter("http://127.0.0.1:8080/nosuch", debug=1)
    r.report_status(jobsub_job_id="12345.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.cleanup()
