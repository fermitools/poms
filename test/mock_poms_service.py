import socket

from poms.webservice import CalendarPOMS, CampaignsPOMS, DBadminPOMS, FilesPOMS, JobsPOMS, TablesPOMS, TagsPOMS, TaskPOMS, TriagePOMS, UtilsPOMS


class mock_poms_service(object):

    def __init__(self):
        self.path = "/xyzzy"
        self.hostname = socket.getfqdn()
        self.version = "mock_test"
        # global_version = self.version
        self.calendarPOMS = CalendarPOMS.CalendarPOMS()
        self.campaignsPOMS = CampaignsPOMS.CampaignsPOMS(self)
        self.dbadminPOMS = DBadminPOMS.DBadminPOMS()
        self.filesPOMS = FilesPOMS.Files_status(self)
        self.jobsPOMS = JobsPOMS.JobsPOMS(self)
        self.tablesPOMS = TablesPOMS.TablesPOMS(self)
        self.tagsPOMS = TagsPOMS.TagsPOMS(self)
        self.taskPOMS = TaskPOMS.TaskPOMS(self)
        self.triagePOMS = TriagePOMS.TriagePOMS(self)
        self.utilsPOMS = UtilsPOMS.UtilsPOMS(self)
