import DBHandle
import datetime
from poms.webservice.utc import utc
import pytest
from mock_Ctx import Ctx

#
# things we need to test quick_search:
# * a db handle
# * a mock redirect exception,
# * a mock poms_service with a "path", etc.
#

from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception

mp_service = mock_poms_service()


@pytest.fixture(scope="session")
def dbhandle():
    return DBHandle.DBHandle()


@pytest.fixture(scope="session")
def utils_poms():
    return mp_service.utilsPOMS


def test_handle_dates_1(utils_poms):
    # check no min, max, just days
    now = datetime.datetime.now(utc)
    ctx = Ctx()
    # def handle_dates(self, tmin, tmax, tdays, baseurl) For args reference
    (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays) = utils_poms.handle_dates(ctx, baseurl="foo/")

    assert tmax - tmin == datetime.timedelta(days=1)
    assert tmax - now < datetime.timedelta(seconds=1)
    print("nextlink={} prevlink={}".format(nextlink, prevlink))
    assert "/foo/" in nextlink
    assert "/foo/" in prevlink


def test_handle_dates_2(utils_poms):
    # check no min, max, just days
    now = datetime.datetime.now(utc)
    ctx = Ctx(tdays="7")

    (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays) = utils_poms.handle_dates(ctx, baseurl="foo/")

    assert tdays == 7
    assert tmax - tmin == datetime.timedelta(days=7)
    assert tmax - now < datetime.timedelta(seconds=1)


def test_handle_dates_3(utils_poms):

    now = datetime.datetime.now(utc)
    ctx = Ctx(tdays="7")

    (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays) = utils_poms.handle_dates(ctx, baseurl="foo/")

    assert tmax - tmin == datetime.timedelta(days=7)
    assert now - tmax < datetime.timedelta(seconds=1)
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)


def test_handle_dates_4(utils_poms):

    now = datetime.datetime.now(utc)
    ctx = Ctx(tmin=str(now - datetime.timedelta(days=10)), tmax="", tdays="7")

    (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays) = utils_poms.handle_dates(ctx, baseurl="foo/")
    assert tmax - tmin == datetime.timedelta(days=7)
    assert str(now - tmax).startswith("3 days")
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)
    # assert 0


def test_handle_dates_5(utils_poms):

    now = datetime.datetime.now(utc)
    ctx = Ctx(tmin=str(now - datetime.timedelta(days=10)), tmax=str(now - datetime.timedelta(days=6)), tdays="")

    (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays) = utils_poms.handle_dates(ctx, baseurl="foo/")
    print(trange)
    assert trange.find("4.0 days") >= 0
    assert tmax - tmin == datetime.timedelta(days=4)
    assert str(now - tmax).startswith("6 days")
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)
    # assert 0


def test_handle_dates_picker(utils_poms):

    # so the picker sets tmin and tmax, but also passes in tdays as whatever
    # it was, so we need tmax and tmin to trump and recompute tdays...

    now = datetime.datetime.now(utc)
    ctx = Ctx(tmax=str(now), tmin=str(now - datetime.timedelta(days=2)), tdays="4")

    (tmin, tmax, tmin_s, tmax_s, nextlink, prevlink, trange, tdays) = utils_poms.handle_dates(ctx, baseurl="foo/")
    print(trange)
    print(nextlink)
    print(prevlink)
    assert trange.find("2.0 days") >= 0
    assert tmax - tmin == datetime.timedelta(days=2)
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)
    # assert 0
