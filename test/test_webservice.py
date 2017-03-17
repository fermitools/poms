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
    proc = utils.setUpPoms()
    yield True
    utils.tearDownPoms(proc)


class TestMethods(unittest.TestCase):
    def test_dashboard(self):
        client.get('index')
        self.assertTrue('Dashboard' in client.text)


    # def test_job_launches_allowed(self):
    #     client.get('set_job_launches?hold=hold')
    #     self.assertTrue('Job launches: hold' in client.text)


    #
    # Top level calls
    #
    def test_dashboard_status(self):
        client.get('index')
        self.assertEqual(client.code, 200)


    def test_active_campaigns(self):
        client.get('show_campaigns')
        self.assertEqual(client.code, 200)
        self.assertTrue('Active Campaign Layers' in client.text)


    def test_calendar(self):
        client.get('calendar')
        self.assertEqual(client.code, 200)
        self.assertTrue('Downtime Calendar' in client.text)


    def test_experiment_edit(self):
        client.get('experiment_edit')
        self.assertEqual(client.code, 200)
        self.assertTrue('Experiment Authorization' in client.text)


    def test_user_edit(self):
        client.get('user_edit')
        self.assertEqual(client.code, 200)
        self.assertTrue('User Authorization' in client.text)


    def test_public_campaigns(self):
        client.get('show_campaigns?experiment=public')
        self.assertEqual(client.code, 200)
        self.assertTrue('Active Campaign Layers' in client.text)


    def test_inactive_campaigns(self):
        client.get('show_campaigns?active=False')
        self.assertEqual(client.code, 200)
        self.assertTrue('InActive Campaign Layers' in client.text)


    def test_campaign_edit(self):
        client.get('campaign_edit')
        self.assertEqual(client.code, 200)
        self.assertTrue('Campaign Layers' in client.text)


    def test_campaign_definition_edit(self):
        client.get('campaign_definition_edit')
        self.assertEqual(client.code, 200)
        self.assertTrue('Job Types' in client.text)


    def test_launch_template_edit(self):
        client.get('launch_template_edit')
        self.assertEqual(client.code, 200)
        self.assertTrue('Launch Templates' in client.text)


    def test_job_table_1d(self):
        client.get('job_table?tdays=1')
        self.assertEqual(client.code, 200)
        self.assertTrue('Jobs' in client.text)


    def test_failed_jobs_by_whatever_1d(self):
        client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=name')
        self.assertEqual(client.code, 200)
        self.assertTrue('Jobs by user_exe_exit_code,name,experiment' in client.text)


    def test_failed_jobs_by_whatever_7d(self):
        client.get('failed_jobs_by_whatever?tdays=7&f=user_exe_exit_code&f=name')
        self.assertEqual(client.code, 200)
        self.assertTrue('Jobs by user_exe_exit_code,name,experiment' in client.text)


    def test_failed_jobs_by_whatever_1d_2(self):
        client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=node_name')
        self.assertEqual(client.code, 200)
        self.assertTrue('Jobs by user_exe_exit_code,node_name,experiment' in client.text)


    def test_failed_jobs_by_whatever_1d_3(self):
        client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=cpu_type')
        self.assertEqual(client.code, 200)
        self.assertTrue('Jobs by user_exe_exit_code,cpu_type,experiment' in client.text)


    #
    # Next layer calls
    #


if __name__ == '__main__':
    p = utils.setUpPoms()
    try:
        unittest.main(verbosity=2)
    finally:
        utils.tearDownPoms(p)
