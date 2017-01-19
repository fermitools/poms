import DBHandle
import datetime
from webservice.utc import utc


#
# things we need to test quick_search: 
# * a db handle
# * a mock redirect exception,
# * a mock poms_service with a "path", etc.
#

dbh = DBHandle.DBHandle()
from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception

mps = mock_poms_service()

def test_quick_search_1():
    got_exception = False
    exception_url = None
    up = mps.utilsPOMS

    try:
        up.quick_search(dbh.get(), mock_redirect_exception, "12249611.0@fifebatch1.fnal.gov")
    except mock_redirect_exception as e:
        got_exception = True
        exception_url = e.what()

    assert(got_exception)
    assert(exception_url.startswith("/xyzzy/"))

def test_handle_dates_1():
    up = mps.utilsPOMS

    # check no min, max, just days
    now = datetime.datetime.now(utc)
    tmin, tmax, timns, tmaxs, nextlink, prevlink, tranges = up.handle_dates("","","7","/foo")

    assert(tmax - tmin == datetime.timedelta(days = 7))
    assert(tmax - now < datetime.timedelta(seconds = 1))

