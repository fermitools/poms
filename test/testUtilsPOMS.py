import DBHandle
import exceptions
import datetime
from webservice.UtilsPOMS import UtilsPOMS
from webservice.utc import utc


#
# things we need to test quick_search: 
# * a db handle
# * a fake redirect exception,
# * a fake poms_service with a "path"...
#

dbh = DBHandle.DBHandle()

class fake_redirect_exception(exceptions.Exception):
    def __init__(self, url ):
        self.url = url

    def what(self):
        return self.url

class fake_poms_service:
    def __init__(self):
        self.path="http://xyzzy/"

def test_quick_search_1():
    got_exception = False
    exception_url = None
    up = UtilsPOMS(fake_poms_service())

    try:
        up.quick_search(dbh.get(), fake_redirect_exception, "12249611.0@fifebatch1.fnal.gov")
    except fake_redirect_exception as e:
        got_exception = True
        exception_url = e.what()

    assert(got_exception)
    assert(exception_url.startswith("http://xyzzy/"))

def test_handle_dates_1():
    up = UtilsPOMS(fake_poms_service())

    # check no min, max, just days
    now = datetime.datetime.now(utc)
    tmin, tmax, timns, tmaxs, nextlink, prevlink, tranges = up.handle_dates("","","7","/foo")

    assert(tmax - tmin == datetime.timedelta(days = 7))
    assert(tmax - now < datetime.timedelta(seconds = 1))

