import math
import poms.webservice.logit as logit
from poms.webservice.poms_model import CampaignStage
from strip_parser import ConfigParser

config = ConfigParser()

class nfiles:
    """
       This type, when filled out as nfiles(n) or nfiles_n for some integer
       n, will slice the dataset into parts of n files using the stride/offset
       expressions.  This does not work so well for dynamic datasets whose
       contents are changing, for them try "drainingn"
    """

    def __init__(self, ctx, cs, test=False):
        self.cs = cs
        self.db = ctx.db
        self.dmr_service = ctx.dmr_service
        self.test = test
        if self.test:
            self.last_split = self.cs.last_split_test
        else:
            self.last_split = self.cs.cs_last_split
        try:
            self.n = int(cs.cs_split_type[7:].strip(")")) if not test else int(cs.test_split_type[7:].strip(")"))
        except:
            raise SyntaxError("unable to parse integer parameter from '%s'" % cs.cs_split_type if not test else cs.test_split_type)

    def params(self):
        return ["n"]

    def peek(self):
        if not self.last_split:
            self.last_split = 0
            
        total_files = self.dmr_service.metacat_client.query(self.cs.data_dispatcher_dataset_query, summary="count").get("count", 0)
        if total_files == 0:
            raise StopIteration
        
        query = "%s ordered skip %d limit %d" % (self.cs.data_dispatcher_dataset_query, self.last_split * self.n, self.n)
        project_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        if len(project_files) == 0:
            raise StopIteration
        
        
        project_name = "%s | nfiles(%s) | %d of %d" % (f"(Test) {self.cs.name}" if self.test else self.cs.name, self.n, self.last_split + 1, math.ceil(total_files/ self.n))
        
        dd_project = self.dmr_service.create_project(username=self.cs.experimenter_creator_obj.username, 
                                               files=project_files,
                                               experiment=self.cs.experiment,
                                               role=self.cs.vo_role,
                                               project_name=project_name,
                                               campaign_id=self.cs.campaign_id, 
                                               campaign_stage_id=self.cs.campaign_stage_id,
                                               split_type=self.cs.cs_split_type if not self.test else self.cs.test_split_type,
                                               last_split=self.last_split,
                                               creator=self.cs.experimenter_creator_obj.experimenter_id,
                                               creator_name=self.cs.experimenter_creator_obj.username)
        
        
        return dd_project

    def next(self):
        dd_project = self.peek()
        
        self.last_split = self.last_split + 1
        logit.log("nfiles.next(): created data_dispatcher project with id: %s " % dd_project.project_id)
        
        return dd_project

    def len(self):
        return len(list(self.dmr_service.metacat_client.query(self.cs.data_dispatcher_dataset_query))) / self.n + 1

    def edit_popup(self):
        return "null"