from webclient import WebClient
import datetime
import utils
import pytest


@pytest.fixture(scope="session")
def client():
    return WebClient(base_url="")


@pytest.fixture(scope="session", autouse=True)
def execute_before_any_test():
    proc = utils.setUpPoms()
    yield True
    utils.tearDownPoms(proc)


def test_dashboard(client):
    client.get("index")
    print("got: %s" % client.text)
    assert "POMS" in client.text


#
# Top level calls
#
def test_dashboard_status(client):
    client.get("index/samdev/production")
    print("ieot: %s" % client.text)
    assert client.code == 200


def test_show_campaigns(client):
    client.get("show_campaigns/samdev/production")
    print("got: %s" % client.text)
    assert client.code == 200
    assert "Campaign Name" in client.text


def test_show_campaign_stages(client):
    client.get("show_campaign_stages/samdev/production")
    print("got: %s" % client.text)
    assert client.code == 200


def test_inactive_campaigns(client):
    client.get("show_campaigns/samdev/production?view_inactive=view_inactive")
    print("got: %s" % client.text)
    assert client.code == 200
    assert "Campaign Name" in client.text


def test_campaign_stage_edit(client):
    client.get("campaign_stage_edit/samdev/production")
    print("got: %s" % client.text)
    assert client.code == 200


def campaign_definition_edit(client):
    client.get("campaign_definition_edit/samdev/production")
    print("got: %s" % client.text)
    assert client.code == 200
    assert "Job Types" in client.text


def test_login_setup_edit(client):
    client.get("login_setup_edit/samdev/production")
    print("got: %s" % client.text)
    assert client.code == 200
    assert "Setup" in client.text


def test_campaign_stage_submissions(client):
    client.get("campaign_stage_submissions/samdev/production?campaign_name=fake demo v1.0  w/chars&stage_name=*&campaign_id=1")
    print("got: %s" % client.text)
    assert client.code == 200


def test_campaign_deps_ini(client):
    client.get("campaign_deps_ini/samdev/production?name=fake demo v1.0  w/chars")
    print("got: %s" % client.text)
    assert client.code == 200


def test_campaign_deps(client):
    client.get("campaign_deps/samdev/production?campaign_name=fake demo v1.0  w/chars")
    print("got: %s" % client.text)
    assert client.code == 200


def test_gui_edit(client):
    client.get("gui_wf_edit/samdev/production?campaign=fake demo v1.0  w/chars")
    print("got: %s" % client.text)
    assert client.code == 200


#
# JSON method tests
#


def test_campaign_list_json(client):
    client.get("campaign_list_json")
    print("got: %s" % client.text)
    assert client.code == 200
    assert "[" in client.text


#
# Next layer calls
#
