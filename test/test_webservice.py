from webclient import WebClient
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
    assert 'Dashboard' in client.text


# def test_job_launches_allowed(client):
#     client.get('set_job_launches?hold=hold')
#    assert 'Job launches: hold' in client.text


#
# Top level calls
#
def test_dashboard_status(client):
    client.get('index')
    assert client.code == 200


def test_active_campaigns(client):
    client.get('show_campaigns')
    assert client.code == 200
    assert 'Active Campaign Stages' in client.text


def test_calendar(client):
    client.get('calendar')
    assert client.code == 200
    assert 'Downtime Calendar' in client.text


def test_experiment_edit(client):
    client.get('experiment_edit')
    assert client.code == 200
    assert 'Experiment Authorization' in client.text


def test_user_edit(client):
    client.get('user_edit')
    assert client.code == 200
    assert 'User Authorization' in client.text


def test_public_campaigns(client):
    client.get('show_campaigns?experiment=public')
    assert client.code == 200
    assert 'Active Campaign Stages' in client.text


def test_inactive_campaigns(client):
    client.get('show_campaigns?active=False')
    assert client.code == 200
    assert 'InActive Campaign Stages' in client.text


def test_campaign_edit(client):
    client.get('campaign_edit')
    assert client.code == 200
    assert 'Campaign Stages' in client.text


def test_campaign_definition_edit(client):
    client.get('campaign_definition_edit')
    assert client.code == 200
    assert 'Job Types' in client.text


def test_launch_template_edit(client):
    client.get('launch_template_edit')
    assert client.code == 200
    assert 'Launch Templates' in client.text


def test_job_table_1d(client):
    client.get('job_table?tdays=1')
    assert client.code == 200
    assert 'Jobs' in client.text


def test_failed_jobs_by_whatever_1d(client):
    client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=name')
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,name,experiment' in client.text


def test_failed_jobs_by_whatever_7d(client):
    client.get('failed_jobs_by_whatever?tdays=7&f=user_exe_exit_code&f=name')
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,name,experiment' in client.text


def test_failed_jobs_by_whatever_1d_2(client):
    client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=node_name')
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,node_name,experiment' in client.text


def test_failed_jobs_by_whatever_1d_3(client):
    client.get('failed_jobs_by_whatever?tdays=1&f=user_exe_exit_code&f=cpu_type')
    assert client.code == 200
    assert 'Jobs by user_exe_exit_code,cpu_type,experiment' in client.text


#
# Next layer calls
#
