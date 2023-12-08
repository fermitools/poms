from datetime import datetime
class limitn:
    """
       This type, when filled out as limitn(n)  for some integer
       n, will just make a datset of the  "defname:base with limit n"
       Useful for analysis users who want a limited set
    """

    def __init__(self, ctx, cs, test=False):
        self.test = test
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        try:
            self.n = int(cs.cs_split_type[7:].strip(")")) if not self.test else int(cs.test_split_type[7:].strip(")"))
        except:
            raise SyntaxError("unable to parse integer parameter from '%s'" % cs.cs_split_type if not self.test else cs.test_split_type)
        
        if self.test:
            self.last_split = self.cs.last_split_test
        else:
            self.last_split = self.cs.cs_last_split
            
    def params(self):
        return ["n"]

    def peek(self):
        project_name = "%s | limit(%d) | %s " % (self.cs.name, self.n, datetime.now().strftime("%m/%d/%Y %I:%M%p"))
        query = "%s limit %d" % (self.cs.data_dispatcher_dataset_query, self.n)
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
                                        split_type=self.cs.cs_split_type if not self.test else self.cs.test_split_type,
                                        last_split=self.last_split,
                                        creator=self.cs.experimenter_creator_obj.experimenter_id,
                                        creator_name=self.cs.experimenter_creator_obj.username)
        return dd_project

    def next(self):
        res = self.peek()
        return res

    def len(self):
        return self.n
        return res

    def edit_popup(self):
        return "null"
