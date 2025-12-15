import poms.webservice.logit as logit
class byrun:
    """
       This type, when filled out as byrun(low=2,high=4) will 
       slice the dataset into parts by picking run numbers 2..4
       one run per batch. Bug:  It does not handle empty runs well
    """
    
        
    def __init__(self, ctx, cs, test=False):
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        self.low = 1
        self.high = 999999
        self.test=test
        if self.test:
            self.last_split = self.cs.last_split_test
        else:
            self.last_split = self.cs.cs_last_split
        
        parms = cs.cs_split_type[6:].split(",") if not self.test else cs.test_split_type[6:].split(",")
        low = 1
        for p in parms:
            if p.endswith(")"):
                p = p[:-1]
            if p.startswith("low="):
                self.low = int(p[4:])
            if p.startswith("high="):
                self.high = int(p[5:])
                

    def params(self):
        return ["low=", "high="]

    def peek(self):
        project_name = "%s | byrun(%s -> %s) | run: %d" % (self.cs.name, self.low, self.high, self.last_split)
        query = "%s where core.run_number = %s" % (self.cs.data_dispatcher_dataset_query, self.last_split)
        project_files =  list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        
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
                                               last_split=self.last_split,
                                               creator=self.cs.experimenter_creator_obj.experimenter_id,
                                               creator_name=self.cs.experimenter_creator_obj.username)
        
        return dd_project

    def next(self):
        if self.last_split is None:
            self.last_split = self.low
        if self.last_split >= self.high:
            raise StopIteration
        
        dd_project = self.peek()
        self.last_split = self.last_split + 1
        
        logit.log("nfiles.next(): created data_dispatcher project with id: %s " % dd_project.project_id)
        return dd_project

    def prev(self):
        self.last_split = self.last_split - 1
        dd_project = self.peek()
        return dd_project

    def len(self):
        return self.high - self.low + 1

    def edit_popup(self):
        return "null"
