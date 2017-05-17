from mock.mock import MagicMock
from mock_poms_service import mock_poms_service
from DBHandle import DBHandle
from poms.webservice.poms_model import Service,ServiceDowntime
from webservice.utc import utc
import time
from datetime import datetime, timedelta

from sqlalchemy import desc

mps = mock_poms_service()
dbhandle = DBHandle().get()

import logging
#logger = logging.getLogger('cherrypy.error')       #not used anymore

# ---------------------------------------------------------------------------------
# Simply schedule an event 

def test_schedule_new_downtime():
   
    runThis = True #False True
    if runThis:
     stime = time.time()      #utc   (local+5)
     etime = stime + 3600     # 1 hr downtime

     # Schedule downtime
     service= "fifemon_mu2e_samweb"           #"fifemon_SAM"

     ret = mps.calendarPOMS.add_event(dbhandle, service, stime, etime)
     #print ' test_schedule_new_downtime, service %s, ret =%s' %(service,ret)

     stime_utc = datetime.fromtimestamp(float(stime), tz=utc)
     etime_utc = datetime.fromtimestamp(float(etime), tz=utc)

     #print ' start = %s end=%s ' %(stime_utc,etime_utc)

     res = mps.calendarPOMS.calendar_json(dbhandle, stime_utc,etime_utc, utc, '')

     #for ar in res: print 'json %s' %ar

     assert(str(res).find(service) > 0)
     #assert (1==2)   # Just to force printing on screen..

# ---------------------------------------------------------------------------------
# Check before scheduling another downtime if already in downtime.. 

def test_check_downtime():
   
    runThis=True
    if runThis:
     service= "fifemon_mu2e_samweb"

     # Check if service is already in downtime and not schedule another in same time interval.

     s = dbhandle.query(Service).filter(Service.name == service).first()

     now = time.time()      #utc   (local+5)
     now_utc = datetime.fromtimestamp(float(now), tz=utc)

     sd = dbhandle.query(ServiceDowntime).filter(ServiceDowntime.service_id == s.service_id ).order_by(desc(ServiceDowntime.downtime_started)).first()
     
     if sd:
        if  now_utc > sd.downtime_ended : 
            #print ' %s scheduling new downtime ' %service
            stime = now
            etime = now + 1800     # half hr downtime
            ret = mps.calendarPOMS.add_event(dbhandle, service, stime, etime)

            stime_utc = datetime.fromtimestamp(float(stime), tz=utc)
            etime_utc = datetime.fromtimestamp(float(etime), tz=utc)
            res = mps.calendarPOMS.calendar_json(dbhandle, stime_utc,etime_utc, utc, '')
            #print ' len json = %s ' %len(res)
            assert(str(res).find(service) > 0)

        else: pass #print ' %s already in downtime ,ending at %s ' %(service,sd.downtime_ended)
                
     #assert (1==2)   # Just to force printing on screen..


# ---------------------------------------------------------------------------------
# Force a service to be "bad" so that it will start downtime

def test_update_service_bad():

    runThis=True    
    if runThis:
     #print ' test_update_service_bad '
     service= "fifemon_mu2e_samweb" 

     s = dbhandle.query(Service).filter(Service.name == service).first()

     status="bad"
     p = dbhandle.query(Service).filter(Service.service_id == s.parent_service_id).first()
     #print ' parent name = %s ' %p.name

     ret = mps.calendarPOMS.update_service(dbhandle,service,p.name,status,s.host_site,s.items,s.failed_items,s.description)
     # Check now it is in downtime

     stime = time.time() - 120     #utc   (local+5)
     etime = stime + 120
     stime_utc = datetime.fromtimestamp(float(stime), tz=utc)
     etime_utc = datetime.fromtimestamp(float(etime), tz=utc)

     #print ' check service downtime between =%s end=%s ' %(stime_utc,etime_utc)  

     res = mps.calendarPOMS.calendar_json(dbhandle, stime_utc,etime_utc, utc, '')
     #for ar in res: print ' test_update_service_bad ,json %s' %ar

     
     #reset status to unknown
     s.status="unknown"
     dbhandle.commit()
     assert(str(res).find(service) > 0)
     
     #assert (1==2)   # Just to force printing on screen..

# ---------------------------------------------------------------------------------
# Force a service to be "good" so that it will start downtime

def test_update_service_good():

    runThis=True
    if runThis:
     #print ' test_update_service_good'
     service= "fifemon_mu2e_samweb" 

     s = dbhandle.query(Service).filter(Service.name == service).first()

     status="good"
     p = dbhandle.query(Service).filter(Service.service_id == s.parent_service_id).first()
     #print ' parent name = %s ' %p.name

     ret = mps.calendarPOMS.update_service(dbhandle,service,p.name,status,s.host_site,s.items,s.failed_items,s.description)
     print('ret = %s' %ret)
     assert ('ok' in ret.lower())
     #assert (1==2)   # Just to force printing on screen..

