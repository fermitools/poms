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
        if cs.dataset == "null":
            self.only = cs.dataset
            return
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
                                               creator_name=self.cs.experimenter_creator_obj.username)


            cs.dataset = str(dd_project.data_dispatcher_project_idx)
            self.project_idx = cs.dataset
        else:
            self.only = int(cs.dataset)

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
        return self.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.data_dispatcher_project_idx == self.only).one_or_none()
