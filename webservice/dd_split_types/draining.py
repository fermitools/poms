import poms.webservice.logit as logit
from sqlalchemy import and_
from poms.webservice.poms_model import DataDispatcherSubmission

class draining:
    """
       This type just always returns the same dataset name forever
       assuming you have a draining/recursive definition that gives
       you the work remaining to be done.
    """

    
    def __init__(self, ctx, cs):
        self.db = ctx.db
        self.cs = cs
        self.cs.data_dispatcher_dataset_only = True
        if cs.dataset == "null":
            self.only = cs.dataset
            return
        
        if self.cs.data_dispatcher_dataset_only:
            if self.cs.dataset and self.cs.dataset.isdigit():
                self.only = cs.dataset
            else:
                self.cs.dataset = self.init_project()
                self.only = int(self.cs.dataset)
        else:
            # Leave this here if project actually needed later
            self.get_or_create_project_if_needed(ctx,cs)
            

    def params(self):
        return []

    def peek(self):
        return self.get_project()

    def next(self):
        res = self.peek()
        return res

    def prev(self):
        res = self.peek()
        return res

    def len(self):
        return -1

    def edit_popup(self):
        return "null"
    
    def get_project(self):
        if cs.data_dispatcher_dataset_only:
            return self.only
        else:
            return self.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.data_dispatcher_project_idx == self.only).first()
        
    def init_project(self):
        dd_project = self.dmr_service.store_project(project_id=None, 
                                            worker_timeout=None, 
                                            idle_timeout=None,
                                            username=self.cs.experimenter_creator_obj.username, 
                                            experiment=self.cs.experiment,
                                            role=self.cs.vo_role,
                                            project_name="%s | draining" % self.cs.name,
                                            campaign_id=self.cs.campaign_id, 
                                            campaign_stage_id=self.cs.campaign_stage_id,
                                            split_type=self.cs.cs_split_type,
                                            last_split=self.cs.cs_last_split,
                                            creator=self.cs.experimenter_creator_obj.experimenter_id,
                                            creator_name=self.cs.experimenter_creator_obj.username,
                                            named_query=cs.data_dispatcher_dataset_only)
        return str(dd_project.data_dispatcher_project_idx)
    
    def get_or_create_project_if_needed(self, ctx, cs):
        if not cs.dataset:
                dd_project = ctx.db.query(DataDispatcherSubmission).filter(and_(
                            DataDispatcherSubmission.experiment == self.cs.experiment,
                            DataDispatcherSubmission.vo_role == self.cs.vo_role,
                            DataDispatcherSubmission.campaign_id == self.cs.campaign_id,
                            DataDispatcherSubmission.campaign_stage_id == self.cs.campaign_stage_id,
                            DataDispatcherSubmission.named_dataset == cs.data_dispatcher_dataset_query)).first()
                
                if not dd_project:
                    project_name = "%s | draining" % cs.name
                    project_files = list(ctx.dmr_service.metacat_client.query(cs.data_dispatcher_dataset_query, with_metadata=True))
                    dd_project = ctx.dmr_service.create_project(username=self.cs.experimenter_creator_obj.username, 
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
                                                named_dataset=cs.data_dispatcher_dataset_query)


                cs.dataset = str(dd_project.data_dispatcher_project_idx)
                self.only = int(cs.dataset)
        else:
            self.only = int(cs.dataset)
    
            
