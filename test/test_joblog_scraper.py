
import MockWebservice
import MockCondor_q
import subprocess
import re
import os
import sys
import time
import json
import urllib.request, urllib.parse, urllib.error
import poms

class TestJobsub_q_scraper:

    def setup(self):
        print("setting up...")
        self.mw = MockWebservice.MockWebservice()
        self.mq = MockCondor_q.MockCondor_q()

    def teardown(self):
        print("tearing down...")
        self.mw.close()
        self.mq.close()

    def _do_test(self,fname):

        os.environ['TEST_JOBLOG'] = '%s/test/data/%s' % (os.environ['POMS_DIR'],fname)

        jqs = subprocess.Popen(["%s/job_broker/joblog_scraper.py" % os.environ['POMS_DIR'], "-t"])
        jqs.wait()

        time.sleep(1)

        df = open("%s/test/data/%s" % (os.environ['POMS_DIR'],fname), "r")

        bulk_data = []
        i = 0

        j = 0
        for line in df:

            if i >= len(bulk_data):
                try:
                    data_log = next(self.mw.log)
                    post_log = next(self.mw.log)
                except StopIteration:
                    break
                post_data =  json.loads(data_log[12:-1])
                bulk_data = json.loads(urllib.parse.unquote_plus(post_data['data']))
                print("got bulk_data:" , bulk_data)
                i = 0

            post_data =  bulk_data[i]

            print("line:", line)
            print("post_data", post_data)

            # need checks for log stuff..
            assert(post_log.find("bulk_update_job") > 0)
            assert(line.find(post_data['jobsub_job_id'].replace('%40','@')) > 0)
            i = i + 1
            j = j + 1


    def test_joblog_1(self):
        self._do_test('joblog_1')


