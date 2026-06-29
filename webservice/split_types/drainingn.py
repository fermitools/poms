import poms.webservice.logit as logit
import time
import uuid


class drainingn:
    """
       This type, when filled out as drainign(n) for some integer
       n, will pull at most n files at a time from the dataset
       and deliver them on each iteration, keeping track of the
       delivered files with a snapshot.  This means it works well
       for datasets that are growing or changing from under it.
    """

    def __init__(self, cs, samhandle, dbhandle, test=False):
        self.test = test
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.dataset = cs.dataset
        self.n = int(cs.cs_split_type[10:].strip(")")) if not self.test else int(cs.test_split_type[10:].strip(")"))

    def params(self):
        return ["nfiles"]

    def peek(self):
        if self.test:
            if not self.cs.last_split_test:
                self.cs.last_split_test = 0
            ls = self.cs.last_split_test
        else:
            if not self.cs.cs_last_split:
                self.cs.cs_last_split = 0
            ls = self.cs.cs_last_split
        if ls != 0:
            snapshotbit = "minus snapshot_id %d" % ls
        else:`
            snapshotbit = ""

        new = self.cs.dataset + "_slice_%i_stage_%d" % (ls, self.n)
        self.samhandle.create_definition(
            self.cs.experiment, new, "defname: %s  %s  with limit %d " % (self.cs.dataset, snapshotbit, self.n)
        )

        if self.samhandle.count_files(self.cs.job_type_obj.experiment, "defname:" + new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        newfullname = res.replace("slice", "full") + "_%s" % int(time.time())
        snap1 = self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, res)
        if self.test:
            ls = self.cs.last_split_test
        else:
            ls = self.cs.cs_last_split

        if ls != 0:
            self.samhandle.create_definition(
                self.cs.experiment, newfullname, "snapshot_id %s or snapshot_id %s" % (ls, snap1)
            )

            snap = self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, newfullname)
        else:
            snap = snap1

        logit.log("stagedfiles.next(): take_snaphot returns %s " % snap)

        if self.test:
            self.cs.last_split_test = snap
        else:
            self.cs.cs_last_split = snap

        return res

    def len(self):
        return self.samhandle.count_files(self.cs.experiment, "defname:" + self.ds) / self.n + 1
        return res

    def edit_popup(self):
        return "null"
