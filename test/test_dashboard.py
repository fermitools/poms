from webclient import WebClient
import unittest
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('../webservice/poms.ini')

port = config.get('global', 'server.socket_port')
pomspath = config.get('global', 'pomspath')
pomspath = pomspath.replace("'", "")

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






if __name__ == '__main__':
    unittest.main(verbosity=2)
