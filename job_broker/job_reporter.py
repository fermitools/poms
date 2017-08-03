#!/usr/bin/env python

import sys
import os
import re
import requests
import urllib.request, urllib.error, urllib.parse
import http.client
import json
import concurrent.futures
import queue
import _thread
import threading
import time
import traceback
from prometheus_client.bridge.graphite import GraphiteBridge

class job_reporter:
    """
       class to report job status -- now runs several threads to asynchronously report queued items
       given to us, so we don't drop messages from syslog piping to us, etc.
    """
    def __init__(self, report_url, debug = 0, nthreads = 8, namespace = "", bulk= True):
        self.namespace = namespace
        self.rs = requests.Session()
        if namespace != "":
            self.gb = GraphiteBridge(('fermicloud079.fnal.gov', 2003))
            self.gb.start(10,prefix=self.namespace)
        self.bulk = bulk
        self.report_url = report_url
        self.debug = debug
        self.wthreads = []
        self.nthreads = nthreads
        # for bulk updates, do batches of 256 or every 10 seconds
        self.batchsize = 1024
        self.timemax = 10
        if self.bulk:
            self.work = []
            for i in range(nthreads):
                self.work.append(queue.Queue())
                   
                self.wthreads.append(threading.Thread(target=self.runqueue_bulk,args=[i]))
                self.wthreads[i].start()
        else:
            self.work = queue.Queue()
            for i in range(nthreads):
                self.wthreads.append(threading.Thread(target=self.runqueue))
                #self.wthreads[i].daemon = True
                self.wthreads[i].start()

    def log_exception(self, e, uh):
        sys.stderr.write("Exception: ")
        if hasattr(e,'code'):
            sys.stderr.write("HTTP error %d\n" % e.code)
            if uh.text:
                sys.stderr.write("Page Text: \n %s \n" % uh.text)
        elif isinstance(e,requests.exceptions.ReadTimeout):
            sys.stderr.write("Read Timeout, moving on...\n")
            time.sleep(1)
            return 1
        else:
            errtext = str(e)
            traceback.print_exc(file=sys.stderr)
        sys.stderr.write("\n--------\n")
        sys.stderr.flush()

        # don't retry on 401's...
        if hasattr(e,'code') and getattr(e,'code') in [401,404]:
            sys.stderr.write("Not retrying.\n")
            del e
            return 0
        elif hasattr(e,'code'):
            del e
            return 1
        else:
            # connection errors, etc.
            del e
            return 1
        
    def check(self):
        # make sure we still have nthreads reporting threads
        if self.bulk:
            for i in range(self.nthreads):
                if self.work[i].qsize() > 100 * self.batchsize:
                    sys.stderr.write("Seriously Backlogged: qsize %d exiting!\n" % self.work.qsize())
                    os._exit(1)
                   
        else:
             if self.work.qsize() > 100 * self.batchsize:
                sys.stderr.write("Seriously Backlogged: qsize %d exiting!\n" % self.work.qsize())
                os._exit(1)
           
        if self.bulk:
            for i in range(self.nthreads):
                if not(self.wthreads[i].is_alive()):
                    self.wthreads[i].join(0.1)
                    self.wthreads[i] = threading.Thread(target=self.runqueue_bulk, args=[i])
                    self.wthreads[i].start()
        else:
            for i in range(self.nthreads):
                if not(self.wthreads[i].is_alive()):
                    self.wthreads[i].join(0.1)
                    self.wthreads[i] = threading.Thread(target=self.runqueue) 
                    self.wthreads[i].start()

    def bail(self):
        print("thread: %d -- bailing" % _thread.get_ident())
        raise KeyboardInterrupt("just quitting a thread")

    def runqueue_bulk(self, which):
        lastsent = time.time()
        bail = False
        while not bail:
            if self.work[which].qsize() > self.batchsize or time.time() - lastsent > self.timemax:

                if self.debug: sys.stderr.write("runqueue_bulk: %d before:qsize is %d\n" % (which,self.work[which].qsize())); sys.stderr.flush()

                batch = []
                for i in range(self.batchsize):
                    try:
                        d = self.work[which].get(block = False)
                    except queue.Empty:
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

                lastsent = time.time()
                self.bulk_update(batch)

                if self.debug: sys.stderr.write("\nrunqueue_bulk: %d after: qsize is %d\n" % (which ,self.work[which].qsize()))
            else:
                time.sleep(1 * which)


    def runqueue(self):
        while 1:
            try:
                d = self.work.get(block = True)
                d['f'](*d['args'], **d['kwargs']) 
            except KeyboardInterrupt:
                break
            except:
                print("Unhandled exception", sys.exc_info())
                time.sleep(1)
                pass
  
    def cleanup(self):
        # first tell threads to exit
        if self.bulk:
            for wth in self.wthreads:
                self.work[wth].put({'f': job_reporter.bail, 'args': [self], 'kwargs':{}})
        else:
            for wth in self.wthreads:
                self.work.put({'f': job_reporter.bail, 'args': [self], 'kwargs':{}})

        # then wait for them
        for wth in self.wthreads:
            wth.join()

    def report_status(self,  jobsub_job_id = '', task_id = '', status = '' , cpu_type = '', slot='', **kwargs ):
        self.check()
        if self.bulk:
           q = self.work[int(task_id) % self.nthreads]
        else:
           q = self.work

        q.put({'f':job_reporter.actually_report_status, 'args': [ self, jobsub_job_id, task_id, status, cpu_type, slot], 'kwargs': kwargs})
 
    def bulk_update(self, batch):

        if self.debug: sys.stderr.write("bulk_update: %d\n\n" % len(batch))
        if self.debug: sys.stderr.write("%s\n\n" % repr(batch))

        if len(batch) == 0:
             if self.debug: sys.stderr.write("bulk_update: completed\n")
             return

        data = {'data': json.dumps(batch)}
        
        retries = 3
          
        res = None

        while retries > 0:

            uh = None

            if retries < 3:
                sys.stderr.write("Retrying...\n")

            try:
                #
                # in an unscientific sampling, 90% of updates were under 3 secons
                # then a separate batch at 30 and 90 seconds split the other 10%
                # so we bail after 3 seconds.  The request actually continues, 
                # and probablly enters the data, but we go on..
                #
                uh = self.rs.post(self.report_url + "/bulk_update_job", data = data, timeout=10)
                res = uh.text

                if self.debug: sys.stderr.write("response: %s\n" % res)
                retries = 0

            except (KeyboardInterrupt):
                raise

            except (Exception) as e:
                if self.log_exception(e, uh):
                    #time.sleep(1) not keeping up... don't delay so much
                    retries = retries - 1
                else:
                    retries = 0

            finally:
                if uh:
                    uh.close()

        if self.debug: sys.stderr.write("bulk_update: completed\n")
        return res


    def actually_report_status(self, jobsub_job_id = '', task_id = '', status = '' , cpu_type = '', slot='', **kwargs ):
        data = {}
        data['task_id'] = task_id
        data['jobsub_job_id'] = jobsub_job_id
        data['cpu_type'] = cpu_type
        data['slot'] = slot
        data['status'] = status
        data.update(kwargs)

        if self.debug:
           sys.stdout.write("%s: reporting: %s\n" %  (time.asctime(), data) )
           sys.stdout.flush()

        retries = 3
          
        res = None
      
        while retries > 0:
            uh = None
            try:
                uh = self.rs.post(self.report_url + "/update_job", data = data, timeout=5)
                res = uh.text

                if self.debug: sys.stderr.write("response: %s\n" % res)
                retries = 0

            except (Exception) as e:
                if self.log_exception(e, uh):
                    time.sleep(5)
                    retries = retries - 1
                else:
                    break
            finally:
                if uh:
                    uh.close()
        return res

if __name__ == '__main__':
    print("self test:")
    r = job_reporter("http://127.0.0.1:8080/poms", debug=1)
    r.report_status(jobsub_job_id="12345.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12346.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12347.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12348.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    print("started reports:")
    sys.stdout.flush()
    r.cleanup()
    r = job_reporter("http://127.0.0.1:8080/nosuch", debug=1)
    r.report_status(jobsub_job_id="12345.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.cleanup()
