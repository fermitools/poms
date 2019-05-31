import socket

from poms.webservice import CampaignsPOMS, DBadminPOMS, FilesPOMS, TablesPOMS, TagsPOMS, MiscPOMS, StagesPOMS, SubmissionsPOMS, UtilsPOMS, JobsPOMS, Permissions


class mock_poms_service(object):
    def __init__(self):
        self.path = "/xyzzy"
        self.hostname = socket.getfqdn()
        self.version = "mock_test"
        # global_version = self.version
        self.campaignsPOMS = CampaignsPOMS.CampaignsPOMS(self)
        self.dbadminPOMS = DBadminPOMS.DBadminPOMS()
        self.filesPOMS = FilesPOMS.FilesStatus(self)
        self.jobsPOMS = JobsPOMS.JobsPOMS(self)
        self.miscPOMS = MiscPOMS.MiscPOMS(self)
        self.permissions = Permissions.Permissions()
        self.stagesPOMS = StagesPOMS.StagesPOMS(self)
        self.submissionsPOMS = SubmissionsPOMS.SubmissionsPOMS(self)
        self.tablesPOMS = TablesPOMS.TablesPOMS(self)
        self.tagsPOMS = TagsPOMS.TagsPOMS(self)
        self.utilsPOMS = UtilsPOMS.UtilsPOMS(self)
