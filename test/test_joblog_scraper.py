
import MockWebservice
import MockCondor_q
import subprocess
import re
import os
import sys
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

    def _do_test(self,fname):

        os.environ['TEST_JOBLOG'] = '%s/test/data/%s' % (os.environ['POMS_DIR'],fname)

        jqs = subprocess.Popen(["%s/job_broker/joblog_scraper.py" % os.environ['POMS_DIR'], "-t"])
        jqs.wait()

        time.sleep(1)

        df = open("%s/test/data/%s" % (os.environ['POMS_DIR'],fname), "r")
        active_log_line = self.mw.log.next()

        for line in df:
            try:
                post_log = self.mw.log.next()
                data_log = self.mw.log.next()
            except StopIteration:
                break


            print "-----\n", line, data_log, post_log
            sys.stdout.flush()

            post_data =  json.loads(data_log[12:-1])
            # need checks for log stuff..
            assert(post_log.find("update_job") > 0)
            assert(line.find(post_data['jobsub_job_id'].replace('%40','@')) > 0)
 

    def test_joblog_1(self):
        self._do_test('joblog_1')

    def test_joblog_2(self):
        self._do_test('joblog_2')

