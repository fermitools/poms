import uuid

class mod:
    """
       This type, when filled out as mod(n) or mod_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.
    """

    def __init__(self, ctx, cs):
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        self.m = int(cs.cs_split_type[4:].strip(")"))

    def params(self):
        return ["modulus"]

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
        if self.cs.cs_last_split >= self.m:
            raise StopIteration

        project_name = "%s | mod(%d) | Slice: %d" % (self.cs.name, self.m, self.cs.cs_last_split)
        
        # Metacat doesn't have a modulus/stride operation so we split the dataset files into buckets of size (m)
        # and select the bucket matching our current cs_last_split value
        query = "%s ordered" % (self.cs.data_dispatcher_dataset_query)
        project_files = self.get_slice_of(list(self.dmr_service.metacat_client.query(query, with_metadata=True)))
        
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
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split + 1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split - 1
        res = self.peek()
        return res

    def len(self):
        return self.m
    
    def get_slice_of(self, files):
        if len(files) == 0:
            raise StopIteration
        sliced_project_files = {}
        slice = 0
        for i in range(0, len(files)):
            if slice in sliced_project_files and len(sliced_project_files[slice]) >= self.m:
                slice += 1
            if slice not in sliced_project_files:
                sliced_project_files[slice] = []
            sliced_project_files[slice].append(files[i])
        return sliced_project_files[self.cs.cs_last_split]

    def edit_popup(self):
        return "null"
