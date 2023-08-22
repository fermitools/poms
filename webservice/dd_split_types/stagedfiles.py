import time

class stagedfiles:
    """
       This type, when filled out as staged_files(n) or mod_n for some integer
       n, will watch the project that is in its input for consumed (staged) 
       files and deliver them on each iteration
    """

    def __init__(self, ctx, cs):
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        self.stage_project = cs.dataset
        self.n = int(cs.cs_split_type[12:].strip(")"))

    def params(self):
        return []
    
    def get_project_id(self):
        try:
            project_id = int(self.cs.data_dispatcher_dataset_query)
        except ValueError:
            raise ValueError("Please use a numerical value in the 'data_dispatcher_dataset_query' field in the campaign stage editor. This value should represent an existing project_id")
        if project_id == 0:
            raise ValueError("Please use a numerical value in the 'data_dispatcher_dataset_query' field in the campaign stage editor. This value should represent an existing project_id")
        return project_id
    

    def peek(self):
        last_run = []
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
            project_name = "%s | staged(%d) | Full Run" % (self.cs.name, self.n)
            project_id = self.get_project_id()
        else:
            last_run = {file.get("fid"):True for file in self.dmr_service.get_file_info_from_project_id(self.cs.cs_last_split)}
            project_name = "%s | staged(%d) | Slice: %d" % (self.cs.name, self.n, self.cs.cs_last_split)
        
        
        all_files = [file for file in self.dmr_service.get_file_info_from_project_id(project_id) if file.get("state", None) == "done"]
        
        if len(all_files) == 0:
            raise StopIteration
        
        project_files = [file for file in all_files[0:min(self.n, len(all_files))] if file.get("fid","") not in last_run]
        
        #if self.cs.create_project:
        #    return self.create_project(project_name, project_files)

        return self.create_project(project_name, project_files)
        
    def next(self):
        dd_project = self.peek()
        self.cs.cs_last_split = dd_project.project_id
        return res

    def len(self):
        project_id = self.get_project_id()
        return len(self.dmr_service.dd_client.get_project(project_id).get("file_handles", []))

    def edit_popup(self):
        return "null"
    
    def create_project(self, project_name, project_files):
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
