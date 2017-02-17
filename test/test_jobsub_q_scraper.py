
import MockWebservice
import MockCondor_q
import subprocess
import re
import os
import time
import json
import urllib

class TestJobsub_q_scraper:

    def setup(self):
        print "setting up..."
        self.mw = MockWebservice.MockWebservice()
        self.mq = MockCondor_q.MockCondor_q()

    def teardown(self):
        print "tearing down..."
        self.mw.close()
        self.mq.close()

    def _parse_line(self,line):
        res = {}
        for aeqb in line.split(';'):
            var,val = aeqb.split('=',1)
            res[var] = val
        return res

    def _do_test(self,fname):

        self.map = {
           "0": "Unexplained",
           "1": "Idle",
           "2": "Running",
           "3": "Removed",
           "4": "Completed",
           "5": "Held",
           "6": "Submission_error",
        }

        print "_do_test: ", fname
        self.mq.setoutput(fname)

        jqs = subprocess.Popen(["%s/job_broker/jobsub_q_scraper.py" % os.environ['POMS_DIR'], "-t"])
        jqs.wait()

        time.sleep(1)

        df = open("%s/test/data/%s" % (os.environ['POMS_DIR'],fname), "r")
        active_log_line = self.mw.log.next()
        active_log_line = self.mw.log.next()
        bulk_data = []

        print "got bulk_data:" , bulk_data

        i = 0
        for line in df:

            if i >= len(bulk_data):
		data_log = self.mw.log.next()
		post_log = self.mw.log.next()
		post_data =  json.loads(data_log[12:-1])
		bulk_data = json.loads(urllib.unquote_plus(post_data['data']))
                i = 0

            # did we post what it said?
            line_data = self._parse_line(line)

            print "checking line_data: ", line_data

            assert(bulk_data[i]['jobsub_job_id'].replace('%40','@') == line_data['JOBSUBJOBID'])
            assert(bulk_data[i]['task_id'] == line_data['POMS_TASK_ID'])
            assert(bulk_data[i]['status'] == self.map[line_data['JOBSTATUS']])
            assert(bulk_data[i]['cpu_time'] == line_data['RemoteUserCpu'])

            i = i + 1

    def test_condor_q_out_1(self):
        self._do_test('condor_q_out1')

