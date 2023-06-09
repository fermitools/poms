import time
import uuid


class stagedfiles:
    """
       This type, when filled out as staged_files(n) or mod_n for some integer
       n, will watch the project that is in its input for consumed (staged) 
       files and deliver them on each iteration
    """

    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.stage_project = cs.dataset
        self.n = int(cs.cs_split_type[12:].strip(")"))

    def params(self):
        return []

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
            snapshotbit = ""
        else:
            snapshotbit = "minus snapshot_id %d" % self.cs.cs_last_split

        new = self.cs.dataset + "_slice_%d_stage_%d" % (self.cs.cs_last_split, self.n)
        self.samhandle.create_definition(
            self.cs.experiment,
            new,
            "project_name %s  and consumed_status 'consumed' %s  with limit %d " % (self.stage_project, snapshotbit, self.n),
        )
        if self.samhandle.count_files(self.cs.job_type_obj.experiment, "defname:" + new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        snap1 = self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, res)
        newfullname = res.replace("slice", "full") + "_%s" % int(time.time())
        if self.cs.cs_last_split:
            snapshotbit = "snapshot_id %s or" % self.cs.cs_last_split
            self.samhandle.create_definition(
                self.cs.experiment, newfullname, "snapshot_id %s or snapshot_id %s " % (self.cs.cs_last_split, snap1)
            )
            snap = self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, newfullname)
        else:
            snap = snap1

        
        self.cs.cs_last_split = snap
        return res

    def len(self):
        return self.samhandle.count_files(self.cs.experiment, "defname:" + self.ds) / self.n + 1
        return res

    def edit_popup(self):
        return "null"
