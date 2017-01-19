import socket
import webservice.AccessPOMS  
import webservice.CalendarPOMS 
import webservice.CampaignsPOMS
import webservice.DBadminPOMS
import webservice.FilesPOMS 
import webservice.JobsPOMS 
import webservice.TablesPOMS
import webservice.TagsPOMS 
import webservice.TagsPOMS
import webservice.TaskPOMS
import webservice.TriagePOMS 
import webservice.UtilsPOMS 
import logging 

logger = logging.getLogger('cherrypy.error')


class mock_poms_service:
    def __init__(self):
        self.path="/xyzzy/"
        self.hostname = socket.getfqdn()
        self.version = "mock_test"
        global_version = self.version
        self.accessPOMS = webservice.AccessPOMS.AccessPOMS()
        self.calendarPOMS = webservice.CalendarPOMS.CalendarPOMS()
        self.campaignsPOMS = webservice.CampaignsPOMS.CampaignsPOMS(self)
        self.dbadminPOMS = webservice.DBadminPOMS.DBadminPOMS()
        self.filesPOMS = webservice.FilesPOMS.Files_status(self)
        self.jobsPOMS = webservice.JobsPOMS.JobsPOMS(self)
        self.tablesPOMS = webservice.TablesPOMS.TablesPOMS(self, logger.info)
        self.tagsPOMS = webservice.TagsPOMS.TagsPOMS(self)
        self.taskPOMS = webservice.TaskPOMS.TaskPOMS(self)
        self.triagePOMS= webservice.TriagePOMS.TriagePOMS(self)
        self.utilsPOMS = webservice.UtilsPOMS.UtilsPOMS(self)
