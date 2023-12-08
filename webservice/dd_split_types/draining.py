import poms.webservice.logit as logit
from sqlalchemy import and_
from poms.webservice.poms_model import DataDispatcherSubmission

class draining:
    """
       This type just always returns the same dataset name forever
       assuming you have a draining/recursive definition that gives
       you the work remaining to be done.
    """

    
    def __init__(self, ctx, cs, test=False):
        self.test = test
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        self.cs.data_dispatcher_dataset_only = True
            

    def params(self):
        return []

    def peek(self):
        project_name = "%s | draining | Run %d" % (self.cs.name, self.cs.cs_last_split)
        if self.cs.data_dispatcher_dataset_only:
            dd_project = self.dmr_service.store_project(project_id=None, 
                                                        worker_timeout=None, 
                                                        idle_timeout=None,
                                                        username=self.cs.experimenter_creator_obj.username, 
                                                        experiment=self.cs.experiment,
                                                        role=self.cs.vo_role,
                                                        project_name=project_name,
                                                        campaign_id=self.cs.campaign_id, 
                                                        campaign_stage_id=self.cs.campaign_stage_id,
                                                        split_type=self.cs.cs_split_type if not self.test else self.cs.test_split_type,
                                                        last_split=self.cs.cs_last_split,
                                                        creator=self.cs.experimenter_creator_obj.experimenter_id,
                                                        creator_name=self.cs.experimenter_creator_obj.username,
                                                        named_dataset=self.cs.data_dispatcher_dataset_query)
        else:
            project_files = list(self.dmr_service.metacat_client.query(self.cs.data_dispatcher_dataset_query, with_metadata=True))
            if len(project_files) == 0:
                raise StopIteration
            dd_project = self.dmr_service.create_project(username=self.cs.experimenter_creator_obj.username, 
                                                        files=project_files,
                                                        experiment=self.cs.experiment,
                                                        role=self.cs.vo_role,
                                                        project_name=project_name,
                                                        campaign_id=self.cs.campaign_id, 
                                                        campaign_stage_id=self.cs.campaign_stage_id,
                                                        split_type=self.cs.cs_split_type if not self.test else self.cs.test_split_type,
                                                        last_split=self.cs.cs_last_split,
                                                        creator=self.cs.experimenter_creator_obj.experimenter_id,
                                                        creator_name=self.cs.experimenter_creator_obj.username, 
                                                        named_dataset=self.cs.data_dispatcher_dataset_query)
        return dd_project

    def next(self):
        if self.cs.cs_last_split == None:
            self.cs.cs_last_split = 0
        else:
            self.cs.cs_last_split = self.cs.cs_last_split + 1
        res = self.peek()
        return res

    def prev(self):
        if self.cs.cs_last_split < 1:
            raise StopIteration
        else:
            self.cs.cs_last_split = self.cs.cs_last_split - 1
        res = self.peek()
        return res

    def len(self):
        return -1

    def edit_popup(self):
        return "null"
        
    
            
