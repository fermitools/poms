import poms.webservice.logit as logit
import time
import uuid


class byexistingruns:
    """
       This type, when filled out as byexistingruns() 
       will pick a run from the unprocessed files, and make
       a batch out of all files of that run each time,
       tracking the processed files in a snapshot whose id
       is stored in cs_last_split
    """

    def __init__(self, cs, dd_client, metacat_client, project_id):
        self.cs = cs
        self.dataset = cs.dataset
        self.dd_client = dd_client
        self.metacat_client = metacat_client
        self.project_id = project_id

    def params(self):
        return []

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
            snapshotbit = ""
        else:
            snapshotbit = "minus snapshot_id %d" % self.cs.cs_last_split
        
        project = self.dd_client.get_project_handles(self.project_id, state="initial")
        if not project:
            raise StopIteration
        
        project_handles = project.get("project_handles", [])
        if len(project_handles) == 0:
            raise StopIteration
        
        project_details = files = project.get("project_details", {})
        run_number = project_details.get("POMS_RUN", None)
        if not run_number:
            raise StopIteration
        
        
        #files = self.metacat_client.get_files(["%s%s" % (file.namespace, file.name) for file in project_handles ], with_metadata=True)
        #if len(files) == 0:
        #    raise StopIteration
        #meta1 = files[0].get("metadata", {})
        #df['result'] = df['string_to_check'].apply(lambda x: [val for key, val in substrings.items() if key in x][0])
        #if not md.get("runs", None):
        #    raise StopIteration

        run_number = "%d.%04d" % (run_number[0][0], run_number[0][1])
        new = self.cs.dataset + "_slice_%i_run_%s" % (self.cs.cs_last_split, run_number)

        self.samhandle.create_definition(
            self.cs.experiment, new, "defname: %s  %s and run_number %s " % (self.cs.dataset, snapshotbit, run_number)
        )

        return new

    def next(self):
        res = self.peek()
        newfullname = res.replace("slice", "full") + "_%s" % int(time.time())
        snap1 = self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, res)
        if self.cs.cs_last_split:
            self.samhandle.create_definition(
                self.cs.experiment, newfullname, "snapshot_id %s or snapshot_id %s" % (self.cs.cs_last_split, snap1)
            )

            snap = self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, newfullname)
        else:
            snap = snap1

        logit.log("stagedfiles.next(): take_snaphot returns %s " % snap)

        self.cs.cs_last_split = snap

        return res

    def len(self):
        # WAG of 2 files per run...
        return self.samhandle.count_files(self.cs.experiment, "defname:" + self.ds) / 2

    def edit_popup(self):
        return "null"
