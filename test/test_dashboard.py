from webclient import WebClient
import unittest
import ConfigParser
import time
import subprocess
import sys


config = ConfigParser.RawConfigParser()
config.read('../webservice/poms.ini')

port = config.get('global', 'server.socket_port')
pomspath = config.get('global', 'pomspath')
pomspath = pomspath.replace("'", "")
pidpath = config.get('global', 'log.pidfile')[1:-1]
base_url = "http://localhost:"+port+pomspath+"/"


client = WebClient(base_url) 


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
    with open(pidpath, 'r') as f:
        pid = f.readline()
    f.close()
    try:
        proc = subprocess.Popen("kill " + pid, shell=True)
    except OSError as e:
        print >>sys.stderr, "Excecution failed:", e



if __name__ == '__main__':
    unittest.main(verbosity=2)
