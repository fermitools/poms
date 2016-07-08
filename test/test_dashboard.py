from webclient import WebClient
import unittest
import time
import subprocess
import sys
import utils


client = WebClient(base_url='')


class indexMethods(unittest.TestCase):

    def test_dashboard(self):
        client.get('index')
        self.assertTrue('Dashboard' in client.text)


    def test_job_launches_allowed(self):
        client.get('set_job_launches?hold=hold')
        self.assertTrue('Job launches: hold' in client.text)


    def test_dashboard_status(self):
        client.get('index')
        self.assertEqual(client.code, 200)



def setUpModule():
    print "************* SETTING UP POMS *************"
    try:
        proc = subprocess.Popen("cd ../ && source /fnal/ups/etc/setups.sh && setup -. poms && cd webservice/ && python service.py &", shell=True)
    except OSError as e:
        print >>sys.stderr, "Execution failed:", e
    time.sleep(10)



def tearDownModule():
    print "************* TEARING DOWN POMS *************"
    pid = utils.get_pid()
    try:
        proc = subprocess.Popen("kill " + pid, shell=True)
    except OSError as e:
        print >>sys.stderr, "Excecution failed:", e



if __name__ == '__main__':
    unittest.main(verbosity=2)
