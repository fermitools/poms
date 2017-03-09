#!/usr/bin/env python

import sys
import os
import re
import requests
import urllib2
import httplib
import json
import concurrent.futures
import Queue
import thread
import threading
import time
import traceback
from prometheus_client.bridge.graphite import GraphiteBridge

class job_reporter:
    """
       class to report job status -- now runs several threads to asynchronously report queued items
       given to us, so we don't drop messages from syslog piping to us, etc.
    """
    def __init__(self, report_url, debug = 0, nthreads = 3, namespace = "", bulk= True):
        self.namespace = namespace
        self.rs = requests.Session()
        if namespace != "":
            self.gb = GraphiteBridge(('fermicloud079.fnal.gov', 2003))
            self.gb.start(10,prefix=self.namespace)
        self.bulk = bulk
        self.report_url = report_url
        self.debug = debug
        self.work = Queue.Queue()
        self.wthreads = []
        self.nthreads = nthreads
        # for bulk updates, do batches of 50 or every 10 seconds
        self.batchsize = 50
        self.timemax = 10
        if self.bulk:
            self.wthreads.append(threading.Thread(target=self.runqueue_bulk))
	    self.wthreads[0].start()
        else:
	    for i in range(nthreads):
		self.wthreads.append(threading.Thread(target=self.runqueue))
		#self.wthreads[i].daemon = True
		self.wthreads[i].start()

    def check(self):
        # make sure we still have nthreads reporting threads
        if self.bulk:
	    if not(self.wthreads[0].isAlive()):
		self.wthreads[0].join(0.1)
		self.wthreads[0] = threading.Thread(target=self.runqueue)
		self.wthreads[0].start()
        else:
	    for i in range(self.nthreads):
		if not(self.wthreads[i].isAlive()):
		    self.wthreads[i].join(0.1)
		    self.wthreads[i] = threading.Thread(target=self.runqueue) 
		    self.wthreads[i].start()

    def bail(self):
        print "thread: %d -- bailing" % thread.get_ident()
        raise KeyboardInterrupt("just quitting a thread")

    def runqueue_bulk(self):
        lastsent = time.time()
        bail = False
        while not bail:
            if self.work.qsize() > self.batchsize or time.time() - lastsent > self.timemax:
                batch = []
                for i in range(self.batchsize):
                    try:
                        d = self.work.get(block = False)
                    except Queue.Empty:
                        break

                    if d['f'] == job_reporter.bail:
                         bail = True
                         break

                    a = {}
                    try:
			for k in ('self', 'jobsub_job_id', 'task_id', 'status','cpu_type', 'slot'):
			    a[k] = d['args'].pop(0)
                    except:
                        pass

                    del a['self']
		    a.update(d['kwargs'])

                    batch.append(a)

                self.bulk_update(batch)
                lastsent = time.time()
            else:
                time.sleep(1)

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

    def bulk_update(self, batch):

	if self.debug: sys.stderr.write("bulk_update: %s\n" % repr(batch))

        data = {'data': json.dumps(batch)}
        
        retries = 3
          
        uh = None
        res = None
      
        while retries > 0:
     	    try:
	        uh = self.rs.post(self.report_url + "/bulk_update_job", data = data)
		res = uh.text
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
		sys.stderr.write("Unknown Exception:" + errtext + repr(sys.exc_info()))
                sys.stderr.write(traceback.format_exc())
		sys.stderr.write("\n--------\n")
                sys.stderr.flush()
                raise

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
		uh = self.rs.post(self.report_url + "/update_job", data = data)
		res = uh.text
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
