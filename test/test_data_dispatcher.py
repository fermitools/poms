
class DataDispatcherTest:
    def __init__(self, ctx, project_id):
        self.ctx = ctx
        self.project_id = project_id
        
        
    def complete_n_files(self, n):
        for x in range(0, n):
            file = self.ctx.dmr_service.client.next_file(self.project_id)
            if file == False:
                return
            elif file == True:
                pass
            else:
                self.ctx.dmr_service.client.file_done(self.project_id, file.did)
                
    def fail_n_files(self, n):
        for x in range(0, n):
            file = self.ctx.dmr_service.client.next_file(self.project_id)
            if file == False:
                return
            elif file == True:
                pass
            else:
                self.ctx.dmr_service.client.file_failed(self.project_id, file.did, False)