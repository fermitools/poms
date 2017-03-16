from webclient import WebClient
import unittest
import time
#import subprocess
import sys
import utils
import pytest


client = WebClient(base_url='')


@pytest.fixture(scope="session", autouse=True)
#@pytest.fixture(scope="module")
def execute_before_any_test():
    utils.setUpPoms()
    yield True
    utils.tearDownPoms()


class TestMethods(unittest.TestCase):
    def test_dashboard(self):
        client.get('index')
        self.assertTrue('Dashboard' in client.text)


    def test_job_launches_allowed(self):
        client.get('set_job_launches?hold=hold')
        self.assertTrue('Job launches: hold' in client.text)


    def test_dashboard_status(self):
        client.get('index')
        self.assertEqual(client.code, 200)


'''
if __name__ == '__main__':
    utils.setUpPoms()
    try:
        unittest.main(verbosity=2)
    finally:
        utils.tearDownPoms()
'''
