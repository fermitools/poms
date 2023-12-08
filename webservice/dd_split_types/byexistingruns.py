import poms.webservice.logit as logit
import time
from sqlalchemy import and_
from poms.webservice.poms_model import DataDispatcherSubmission

class byexistingruns:
    """
       This type, when filled out as byexistingruns() 
       will pick a run from the unprocessed files, and make
       a batch out of all files of that run each time,
       tracking the processed files in a snapshot whose id
       is stored in cs_last_split
    """

    def __init__(self, ctx, cs, test=False):
        self.test = test
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        self.db = ctx.db
        if self.test:
            self.last_split = self.cs.last_split_test
        else:
            self.last_split = self.cs.cs_last_split


    def params(self):
        return []

    def peek(self):
        query = self.cs.data_dispatcher_dataset_query
        fids_already_processed = []
        if self.cs.last_split > 0:
            for project in self.db.query(DataDispatcherSubmission).filter(and_(
                        DataDispatcherSubmission.archive == False,
                        DataDispatcherSubmission.experiment == self.cs.experiment,
                        DataDispatcherSubmission.vo_role == self.cs.vo_role,
                        DataDispatcherSubmission.campaign_id == self.cs.campaign_id,
                        DataDispatcherSubmission.campaign_stage_id == self.cs.campaign_stage_id,
                        DataDispatcherSubmission.last_split < self.cs.last_split)).all():
                
                fids = [file.get("fid") for file in self.dmr_service.get_file_info_from_project_id(project.project_id) if file.get("state") != "done"]
                fids_already_processed.extend(fids)

        if len(fids_already_processed) > 0:
            query += " - (fids %)" % ", ".join(fids_already_processed)
        
        files = list(self.dmr_service.metacat_client.query("%s limit 1" % query, with_metadata=True))
        md = files[0].get("metadata",None) if files and len(files) > 0 else None
        if md is None or not md.get("core.runs",None):
            raise StopIteration

        run_number = "%d.%04d" % (md["core.runs"][0][0], md["core.runs"][0][1])
        query += "and core.run_number = %s" % run_number
        
        project_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        
        if len(project_files) == 0:
            raise StopIteration
        
        if self.last_split == 0:
            project_name = "%s | byexistingrun(%s)-full | %s" % (self.cs.name, run_number, int(time.time()))
        else:
            project_name = "%s | byexistingrun(%s) | slice %s" % (self.cs.name, run_number, self.last_split)
        
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
        if self.last_split is None:
            self.last_split = 0
        else:
            self.last_split= self.last_split + 1
            
        dd_project = self.peek()
        
        logit.log("stagedfiles.next(): created data_dispatcher project with id: %s " % dd_project.project_id)

        return dd_project

    def len(self):
        # WAG of 2 files per run...
        dd_project = self.db.query(DataDispatcherSubmission).filter(and_(
                        DataDispatcherSubmission.archive == False,
                        DataDispatcherSubmission.experiment == self.cs.experiment,
                        DataDispatcherSubmission.vo_role == self.cs.vo_role,
                        DataDispatcherSubmission.campaign_id == self.cs.campaign_id,
                        DataDispatcherSubmission.campaign_stage_id == self.cs.campaign_stage_id,
                        DataDispatcherSubmission.last_split == self.cs.last_split)).first()
        
        return len(self.dmr_service.get_file_info_from_project_id(dd_project.project_id)) / 2


    def edit_popup(self):
        return "null"
