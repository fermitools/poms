import poms.webservice.logit as logit
import time
from strip_parser import ConfigParser
from sqlalchemy import and_
from poms.webservice.poms_model import DataDispatcherSubmission
config = ConfigParser()

class byexistingruns:
    """
       This type, when filled out as byexistingruns() 
       will pick a run from the unprocessed files, and make
       a batch out of all files of that run each time,
       tracking the processed files in a snapshot whose id
       is stored in cs_last_split
    """

    def __init__(self, cs, dmr_service, dbhandle):
        self.cs = cs
        self.project_id = cs.data_dispatcher_project_id
        self.dataset_query = cs.data_dispatcher_dataset_query
        self.metacat_client = dmr_service.metacat_client
        self.dmr_service = dmr_service
        self.dbhandle = dbhandle
        self.namespace = config.get("Metacat", "SPLIT_TYPE_NAMESPACE")

    def params(self):
        return []

    def peek(self):
        query = self.dataset_query
        
        if self.cs.cs_last_split > 0:
            previous_dataset_definition = self.dbhandle.query(DataDispatcherSubmission.named_dataset).filter(
                    and_(
                        DataDispatcherSubmission.experiment == self.cs.experiment,
                        DataDispatcherSubmission.vo_role == self.cs.vo_role,
                        DataDispatcherSubmission.campaign_id == self.cs.campaign_id,
                        DataDispatcherSubmission.campaign_stage_id == self.cs.campaign_stage_id,
                        DataDispatcherSubmission.last_split == self.cs.last_split -1
                    )
                ).one_or_none()
        
            if previous_dataset_definition:
                query += " - (files from %s)" % (previous_dataset_definition)
        
        files = list(self.metacat_client.query("%s limit 1" % query, with_metadata=True))
        if len(files) == 0:
            raise StopIteration

        md = files[0]
        if not md.get("core.runs", None):
            raise StopIteration

        run_number = "%d.%04d" % (md["core.runs"][0][0], md["core.runs"][0][1])
        query += "and core.runs = %s" % run_number
        
        if self.cs.cs_last_split == 0:
            new_dataset_definition = "%s:campaign_%s_stage_%s_byexistingrun_full_run_%s_%s" % (self.namespace, self.cs.campaign_id, self.cs.campaign_stage_id, run_number, int(time.time()))
        else:
            new_dataset_definition = "%s:campaign_%s_stage_%s_byexistingrun_slice_%s_run_%s" % (self.namespace, self.cs.campaign_id, self.cs.campaign_stage_id, self.cs.cs_last_split, run_number)
        
        try:
            self.dmr_service.create_dataset_definition(self.namespace, new_dataset_definition, query)
        except:
            raise StopIteration
        
        return new_dataset_definition

    def next(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
        else:
            self.cs.cs_last_split = self.cs.cs_last_split + 1
            
        res = self.peek()
        dd_project = self.dmr_service.create_project(username=self.cs.experimenter_creator_obj.username, 
                                               dataset="files from %s" % res,
                                               experiment=self.cs.experiment,
                                               role=self.cs.vo_role,
                                               project_name=res,
                                               campaign_id=self.cs.campaign_id, 
                                               campaign_stage_id=self.cs.campaign_stage_id,
                                               split_type=self.cs.cs_split_type,
                                               last_split=int(self.cs.cs_last_split),
                                               creator=self.cs.experimenter_creator_obj.experimenter_id,
                                               creator_name=self.cs.experimenter_creator_obj.username,
                                               named_dataset=res)
        logit.log("stagedfiles.next(): created data_dispatcher project with id: %s " % dd_project.project_id)

        return res, dd_project

    def len(self):
        # WAG of 2 files per run...
        return self.samhandle.count_files(self.cs.experiment, "defname:" + self.ds) / 2

    def edit_popup(self):
        return "null"
