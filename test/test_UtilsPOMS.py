import DBHandle
import datetime
from poms.webservice.utc import utc
import pytest

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


def test_quick_search_1(dbhandle, utils_poms):
    got_exception = False
    exception_url = None

    try:
        utils_poms.quick_search(dbhandle.get(), mock_redirect_exception, "12249611.0@fifebatch1.fnal.gov")
    except mock_redirect_exception as e:
        got_exception = True
        exception_url = e.what()

    assert got_exception
    assert exception_url.startswith("/xyzzy/")


def test_handle_dates_1(utils_poms):
    # check no min, max, just days
    now = datetime.datetime.now(utc)
    # def handle_dates(self, tmin, tmax, tdays, baseurl) For args reference
    (tmin, tmax, tmin_s, tmax_s,
     nextlink, prevlink,
     trange, tdays) = utils_poms.handle_dates(tmin="", tmax="", tdays="", baseurl="foo/")

    assert (tmax - tmin == datetime.timedelta(days=1))
    assert (tmax - now < datetime.timedelta(seconds=1))
    print("nextlink={} prevlink={}".format(nextlink, prevlink))
    assert '/foo/' in nextlink
    assert '/foo/' in prevlink


def test_handle_dates_2(utils_poms):
    # check no min, max, just days
    now = datetime.datetime.now(utc)

    (tmin, tmax, tmin_s, tmax_s,
     nextlink, prevlink,
     trange, tdays) = utils_poms.handle_dates(tmin="",
                                       tmax="",
                                       tdays="7", baseurl="foo/")

    assert (tdays == 7)
    assert (tmax - tmin == datetime.timedelta(days=7))
    assert (tmax - now < datetime.timedelta(seconds=1))


def test_handle_dates_3(utils_poms):

    now = datetime.datetime.now(utc)

    (tmin, tmax, tmin_s, tmax_s,
     nextlink, prevlink,
     trange, tdays) = utils_poms.handle_dates(tmin="",
                                       tmax=str(now),
                                       tdays="7", baseurl="foo/")

    assert (tmax - tmin == datetime.timedelta(days=7))
    assert (now - tmax < datetime.timedelta(seconds=1))
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)


def test_handle_dates_4(utils_poms):

    now = datetime.datetime.now(utc)

    (tmin, tmax, tmin_s, tmax_s,
     nextlink, prevlink,
     trange, tdays) = utils_poms.handle_dates(tmin=str(now - datetime.timedelta(days=10)),
                                       tmax="",
                                       tdays="7", baseurl="foo/")
    assert (tmax - tmin == datetime.timedelta(days=7))
    assert str(now - tmax).startswith('3 days')
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)
    # assert 0


def test_handle_dates_5(utils_poms):

    now = datetime.datetime.now(utc)

    (tmin, tmax, tmin_s, tmax_s,
     nextlink, prevlink,
     trange, tdays) = utils_poms.handle_dates(tmin=str(now - datetime.timedelta(days=10)),
                                       tmax=str(now - datetime.timedelta(days=6)),
                                       tdays="", baseurl="foo/")
    print(trange)
    assert(trange.find("4.0 days") >= 0)
    assert (tmax - tmin == datetime.timedelta(days=4))
    assert str(now - tmax).startswith('6 days')
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)
    # assert 0

def test_handle_dates_picker(utils_poms):

    # so the picker sets tmin and tmax, but also passes in tdays as whatever
    # it was, so we need tmax and tmin to trump and recompute tdays...

    now = datetime.datetime.now(utc)

    (tmin, tmax, tmin_s, tmax_s,
     nextlink, prevlink,
     trange, tdays) = utils_poms.handle_dates(tmax=str(now),
                                       tmin=str(now - datetime.timedelta(days=2)),
                                       tdays="4", baseurl="foo/")
    print(trange)
    print(nextlink)
    print(prevlink)
    assert(trange.find("2.0 days") >= 0)
    assert (tmax - tmin == datetime.timedelta(days=2))
    # print "tmin='{}', tmax='{}', tmin_s='{}', tmax_s='{}', trange='{}'".format(tmin, tmax, tmin_s, tmax_s, trange)
    # print "now-tmax='{}'".format(now - tmax)
    # assert 0
