import poms.webservice.logit as logit
import poms.webservice.DMRService as shrek
import cherrypy
import time
import uuid
from poms.webservice.poms_model import DataDispatcherSubmission
from sqlalchemy import text


class drainingn_new:
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
        self.dmr_service = ctx.dmr_service  if ctx.dmr_service else shrek.DMRService()
        self.dmr_service.initialize_session(ctx)
        self.dmr_service.set_data_dispatcher_client()
        self.dmr_service.set_metacat_client()
        self.peek_files = []

        logit.log(f"drainingn_new split __init__: ctx.experiment {ctx.experiment}")
        logit.log(f"drainingn_new split __init__: mc: {self.dmr_service.metacat_client} dd: {self.dmr_service.metacat_client}")
  
        self.n = int(cs.cs_split_type[14:].strip(")")) if not self.test else int(cs.test_split_type[14:].strip(")"))
        if self.test:
            self.last_split = self.cs.last_split_test
        else:
            self.last_split = self.cs.cs_last_split

    def params(self):
        return ["nfiles"]

    def peek(self):

        logit.log(f"drainingn_new split peek: mc: {self.dmr_service.metacat_client} dd: {self.dmr_service.metacat_client}")
        if not self.dmr_service.metacat_client:
            if "Shrek" not in cherrypy.session or "mc_client" not in cherrypy.session["Shrek"] or not cherrypy.session["Shrek"]["mc_client"]:
                self.dmr_service = shrek.DMRService()
                self.dmr_service.initialize_session(self.ctx, cron_session=True)
                self.dmr_service.set_data_dispatcher_client()
                self.dmr_service.set_metacat_client()

        if not self.last_split:
            # blank because first time *or* we were reset...
            # use a 'standard' name
            self.last_split = f"poms:drainingn_used_cs_{cs.campaign_stage_id}"
            self.cs.last_split = self.last_split
            self.db.commit()
            # if there is an existing one dataset, we were reset, so
            # remove it to complete the reset
            try:
                existing = self.dmr_service.metacat_client.get_dataset(did=self.last_split)
                self.dmr_service.metacat_client.remove_dataset(did=self.last_split)
            except ValueError:
                # no existing dataset, it really is the first time
                pass

        # now create a fresh dataset
        self.dmr_service.metacat_client.create_dataset(did=self.last_split)
        base_query =self.cs.data_dispatcher_datset_query 
        query = f"{base_query} minus files from {self.last_split} limit {self.n}"
        all_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        if len(all_files) == 0:
            raise StopIteration
        self.peek_files = all_files

        return self.create_project(project_name, all_files, named_dataset = query)
    def next(self):
        dd_project = self.peek()

        # add the files we got to the used dataset:
        self.dmr_service.metacat_client.add_files(self.last_split, self.peek_files)
        dd_project.last_split = dd_project.project_id
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
