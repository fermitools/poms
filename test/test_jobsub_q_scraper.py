
import MockWebservice
import MockCondor_q
import subprocess
import re
import os
import time

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

        print "_do_test: ", fname
        self.mq.setoutput(fname)

        jqs = subprocess.Popen(["%s/job_broker/jobsub_q_scraper.py" % os.environ['POMS_DIR'], "-t"])
        jqs.wait()

        time.sleep(1)

        df = open("%s/test/data/%s" % (os.environ['POMS_DIR'],fname), "r")

        for line in df:
            data_log = self.mw.log.next()
            post_log = self.mw.log.next()
            print "dl:", line
            print "pd:", data_log
            print "ll:", post_log
            
    def test_condor_q_out_1(self):
        self._do_test('condor_q_out1')
        assert(False)

