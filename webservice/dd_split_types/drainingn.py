import poms.webservice.logit as logit
import time
import uuid
from poms.webservice.poms_model import DataDispatcherSubmission


class drainingn:
    """
       This type, when filled out as drainign(n) for some integer
       n, will pull at most n files at a time from the dataset
       and deliver them on each iteration, keeping track of the
       delivered files with a snapshot.  This means it works well
       for datasets that are growing or changing from under it.
    """
    def __init__(self, ctx, cs):
        self.db = ctx.db
        self.cs = cs
        self.cs.data_dispatcher_dataset_only = False
        self.dmr_service = ctx.dmr_service
        self.n = int(cs.cs_split_type[10:].strip(")"))
            
    def params(self):
        return ["nfiles"]

    def peek(self):
        dont_use = []
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
            project_name = "%s | draining(%d) | First Run" % (self.cs.name, self.n)
            query = "%s limit %d" % (self.cs.data_dispatcher_dataset_query, self.n)
            all_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        else:
            previous_subs = [submission.project_id for submission in self.db.query(DataDispatcherSubmission).filter(
                DataDispatcherSubmission.experiment == self.cs.experiment, 
                DataDispatcherSubmission.campaign_stage_id == self.cs.campaign_stage_id,
                DataDispatcherSubmission.split_type == self.cs.cs_split_type,
                DataDispatcherSubmission.project_id != None,
                DataDispatcherSubmission.splits_reset == False,
                DataDispatcherSubmission.archive == False).all()]
            for project_id in previous_subs:
                dont_use.extend([file.get("fid") for file in self.dmr_service.get_file_info_from_project_id(project_id)])
            project_name = "%s | draining(%d) | Slice: %d" % (self.cs.name, self.n, self.cs.cs_last_split)
            if len(dont_use) > 0:
                query = "%s - (fids %s) limit %d" % (self.cs.data_dispatcher_dataset_query, ",".join(list(set(dont_use))), self.n)
            else:
                query = "%s limit %d" % (self.cs.data_dispatcher_dataset_query, ",".join(list(set(dont_use))), self.n)
        all_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        
        if len(all_files) == 0:
            raise StopIteration
        
        project_files = [file for file in all_files[0:min(self.n, len(all_files))] if file.get("fid", "") not in dont_use]
        
        return self.create_project(project_name, project_files, named_dataset = query)

    def next(self):
        dd_project = self.peek()
        self.cs.cs_last_split = dd_project.project_id
        return dd_project

    def len(self):
        return self.ctx.dmr_service.metacat_client.query(self.cs.data_dispatcher_dataset_query, summary="count").get("count",0) / self.n + 1

    def edit_popup(self):
        return "null"
    
    def create_project(self, project_name, project_files, named_dataset):
        dd_project = self.dmr_service.create_project(username=self.cs.experimenter_creator_obj.username, 
                                        files=project_files,
                                        experiment=self.cs.experiment,
                                        role=self.cs.vo_role,
                                        project_name=project_name,
                                        campaign_id=self.cs.campaign_id, 
                                        campaign_stage_id=self.cs.campaign_stage_id,
                                        split_type=self.cs.cs_split_type,
                                        last_split=self.cs.cs_last_split,
                                        creator=self.cs.experimenter_creator_obj.experimenter_id,
                                        creator_name=self.cs.experimenter_creator_obj.username,
                                        named_dataset=named_dataset)
        return dd_project
