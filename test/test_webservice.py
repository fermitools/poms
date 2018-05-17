from webclient import WebClient
import datetime
import utils
import pytest


@pytest.fixture(scope="session")
def client():
    return WebClient(base_url='')


@pytest.fixture(scope="session", autouse=True)
def execute_before_any_test():
    proc = utils.setUpPoms()
    yield True
    utils.tearDownPoms(proc)


def test_dashboard(client):
    client.get('index')
    print("got: %s" % client.text)
    assert 'Dashboard' in client.text


# def test_job_launches_allowed(client):
#     client.get('set_job_launches?hold=hold')
#    assert 'Job launches: hold' in client.text


#
# Top level calls
#
def test_dashboard_status(client):
    client.get('index')
    print("ieot: %s" % client.text)
    assert client.code == 200


def test_active_campaigns(client):
    client.get('show_campaigns')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Active Campaign Stages' in client.text


def test_calendar(client):
    client.get('calendar')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Downtime Calendar' in client.text


def test_experiment_edit(client):
    client.get('experiment_edit')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Experiment Authorization' in client.text


def test_user_edit(client):
    client.get('user_edit')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'User Authorization' in client.text


def test_public_campaigns(client):
    client.get('show_campaigns?experiment=public')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Active Campaign Stages' in client.text


def test_inactive_campaigns(client):
    client.get('show_campaigns?active=False')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'InActive Campaign Stages' in client.text


def test_campaign_edit(client):
    client.get('campaign_edit')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Campaign Stages' in client.text


def test_campaign_definition_edit(client):
    client.get('campaign_definition_edit')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Job Types' in client.text


def test_launch_template_edit(client):
    client.get('launch_template_edit')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Launch Templates' in client.text


def test_job_table_1d(client):
    client.get('job_table?tdays=1')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Jobs' in client.text


def test_failed_jobs_by_whatever_1d(client):
    client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=name')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,name,experiment' in client.text


def test_failed_jobs_by_whatever_7d(client):
    client.get('failed_jobs_by_whatever?tdays=7&f=user_exe_exit_code&f=name')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,name,experiment' in client.text


def test_failed_jobs_by_whatever_1d_2(client):
    client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=node_name')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,node_name,experiment' in client.text


def test_failed_jobs_by_whatever_1d_3(client):
    client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=cpu_type')
    print("got: %s" % client.text)
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,cpu_type,experiment' in client.text


#
# JSON method tests
#

def test_active_jobs(client):
    client.get('active_jobs')
    print("got: %s" % client.text)
    assert client.code == 200
    assert '[' in client.text

def test_output_pending_jobs(client):
    client.get('output_pending_jobs')
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text

def test_json_job_counts(client):
    client.get('json_job_counts?campaign_id=1&tmin={}&tmax={}'.format(
            (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       ))
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text


def test_json_pending_for_campaigns(client):
    client.get('json_pending_for_campaigns?cl=1&tmin={}&tmax={}'.format(
            (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       ))
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text

def test_json_efficiency_for_campaigns(client):
    client.get('json_pending_for_campaigns?cl=1&tmin={}&tmax={}'.format(
            (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       ))
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text

def test_json_project_summary_for_task(client):
    client.get('json_project_summary_for_task?task_id=19')
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text

def test_link_tags(client):
    client.get('link_tags?campaign_id=1&tag_name=foobieblatch&experiment=samdev')
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text

def test_delete_campaigns_tags(client):
    client.get('delete_campaigns_tags?campaign_id=1&tag_id=99&experiment=samdev')
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text

def test_calendar_json(client):
    client.get('calendar_json?_=x&start={}&end={}&timezone=CST'.format(
            (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
          
       ))
    print("got: %s" % client.text)
    assert client.code == 200
    assert '{' in client.text

def test_experiment_members(client):
    client.get('experiment_members?experiment=samdev')
    print("got: %s" % client.text)
    assert '"' in client.text
    assert client.code == 200

def test_campaign_list_json(client):
    client.get('campaign_list_json')
    print("got: %s" % client.text)
    assert client.code == 200
    assert '[' in client.text
#
# Next layer calls
#
