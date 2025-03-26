import poms.webservice.logit as logit
import poms.webservice.DMRService as shrek
import cherrypy
import time
import uuid
from poms.webservice.poms_model import DataDispatcherSubmission
from sqlalchemy import text


class drainingn:
    """
       This type, when filled out as drainign(n) for some integer
       n, will pull at most n files at a time from the dataset
       and deliver them on each iteration, keeping track of the
       delivered files with a snapshot.  This means it works well
       for datasets that are growing or changing from under it.
    """
    def __init__(self, ctx, cs, test=False):
        self.test = test
        self.ctx = ctx
        self.cs = cs
        self.db = ctx.db
        self.cs.data_dispatcher_dataset_only = False
        self.dmr_service = ctx.dmr_service  if ctx.dmr_service else shrek.DMRService(cherrypy.session.get("Shrek", {}))
        self.dmr_service.initialize_session(ctx)
        self.n = int(cs.cs_split_type[10:].strip(")")) if not self.test else int(cs.test_split_type[10:].strip(")"))
        if self.test:
            self.last_split = self.cs.last_split_test
        else:
            self.last_split = self.cs.cs_last_split

    def params(self):
        return ["nfiles"]

    def peek(self):

        if "Shrek" not in cherrypy.session or "mc_client" not in cherrypy.session["Shrek"]:
            self.dmr_service = shrek.DMRService()
            self.dmr_service.initialize_session(self.ctx, cron_session=True)
        if 'mc_client' in cherrypy.session["Shrek"]:
            self.dmr_service.metacat_client = cherrypy.session["Shrek"]['mc_client']

        dont_use = []
        if not self.last_split:
            self.last_split = 0
            project_name = ("TEST | " if self.test else "") + "%s | draining(%d) | First Run" % (self.cs.name, self.n)
            query = "%s limit %d" % (self.cs.data_dispatcher_dataset_query, self.n)
            all_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        else:
            previous_subs = [submission.project_id for submission in self.db.query(DataDispatcherSubmission).filter(
                DataDispatcherSubmission.experiment == self.cs.experiment, 
                DataDispatcherSubmission.campaign_stage_id == self.cs.campaign_stage_id,
                DataDispatcherSubmission.split_type == "%s" % (self.cs.cs_split_type if not self.test else self.cs.test_split_type),
                DataDispatcherSubmission.project_id != None,
                DataDispatcherSubmission.splits_reset == False,
                DataDispatcherSubmission.archive == False).all()]
            for project_id in previous_subs:
                dont_use.extend([file.get("fid") for file in self.dmr_service.get_file_info_from_project_id(project_id)])
            project_name = ("TEST | " if self.test else "") +  "%s | draining(%d) | Slice: %d" % (self.cs.name, self.n, self.last_split)
            if len(dont_use) > 0:
                query = "%s - (fids %s) limit %d" % (self.cs.data_dispatcher_dataset_query, ",".join(list(set(dont_use))), self.n)
            else:
                query = "%s limit %d" % (self.cs.data_dispatcher_dataset_query, self.n)
        all_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))

        if len(all_files) == 0:
            raise StopIteration

        project_files = [file for file in all_files[0:min(self.n, len(all_files))] if file.get("fid", "") not in dont_use]

        return self.create_project(project_name, project_files, named_dataset = query)

    def next(self):
        dd_project = self.peek()

        if self.test:
            self.cs.last_split_test = dd_project.project_id
        else:
            self.cs.cs_last_split = dd_project.project_id
        dd_project.last_split = dd_project.project_id
        self.db.commit()
        return dd_project

    def len(self):
        return self.dmr_service.metacat_client.query(self.cs.data_dispatcher_dataset_query, summary="count").get("count", 0) / self.n + 1

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
                                        split_type=self.cs.cs_split_type if not self.test else self.cs.test_split_type,
                                        creator=self.cs.experimenter_creator_obj.experimenter_id,
                                        creator_name=self.cs.experimenter_creator_obj.username,
                                        named_dataset=named_dataset)
        dd_project.last_split = dd_project.project_id
        return dd_project
