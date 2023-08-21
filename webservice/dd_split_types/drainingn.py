import poms.webservice.logit as logit
import time
import uuid


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
        self.dmr_service = ctx.dmr_service
        self.n = int(cs.cs_split_type[10:].strip(")"))
            
    def params(self):
        return ["nfiles"]

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
            project_name = "%s | draining(%d) | First Run" % (self.cs.name, self.n, self.cs.cs_last_split)
            query = "%s limit %d" % (self.cs.data_dispatcher_dataset_query, self.n)
            project_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        else:
            all_processed = []
            previous_runs = self.db.query(DataDispatcherSubmission).filter(and_(
                        DataDispatcherSubmission.experiment == self.cs.experiment,
                        DataDispatcherSubmission.vo_role == self.cs.vo_role,
                        DataDispatcherSubmission.campaign_id == self.cs.campaign_id,
                        DataDispatcherSubmission.campaign_stage_id == self.cs.campaign_stage_id,
                        DataDispatcherSubmission.split_type == self.cs.cs_split_type,
                        DataDispatcherSubmission.last_split < self.cs.cs_last_split)).all()
            for run in previous_runs:
                all_processed.extend([file.get("fid") for file in self.dmr_service.get_file_info_from_project_id(last_run.project_id)])
            
            project_name = "%s | draining(%d) | Slice: %d" % (self.cs.name, self.n, self.cs.cs_last_split)
            query = "%s - (fids %s) limit %d" % (self.cs.data_dispatcher_dataset_query, ",".join(all_processed), self.n)
            project_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
            
        if len(project_files) == 0:
            raise StopIteration
        
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
                                        creator_name=self.cs.experimenter_creator_obj.username)
                

        return dd_project

    def next(self):
        dd_project = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split + 1
        return dd_project

    def len(self):
        return self.ctx.dmr_service.metacat_client.query(self.cs.data_dispatcher_dataset_query, summary="count").get("count",0) / self.n + 1

    def edit_popup(self):
        return "null"
