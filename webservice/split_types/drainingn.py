import poms.webservice.logit as logit

class drainingn:
    """
       This type, when filled out as staged_files(n) or mod_n for some integer
       n, will watch the project that is in its input for consumed (i.e. staged) files
       and deliver them on each iteration
    """
    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.dataset = cs.dataset
        self.n = int(cs.cs_split_type[10:].strip(')'))

    def params(self):
        return ["nfiles"]

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
            snapshotbit=""
        else:
            snapshotbit="minus snapshot_id %d" % self.cs.cs_last_split

        new = self.cs.dataset + "_slice_%i_stage_%d" % (self.cs.cs_last_split, self.n)
        self.samhandle.create_definition(self.cs.experiment, new,  "defname: %s  %s  with limit %d " % (self.cs.dataset, snapshotbit, self.n))

        if self.samhandle.count_files(self.cs.job_type_obj.experiment, "defname:"+new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        newfullname = res.replace('slice','full')
        snap1 =  self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, res)
        if self.cs.cs_last_split:
            self.cs.cs_last_split
            self.samhandle.create_definition(self.cs.experiment, newfullname, 'snapshot_id %s or snapshot_id %s' % (self.cs.cs_last_split, snap1))

            snap = self.samhandle.take_snapshot(self.cs.job_type_obj.experiment, newfullname)
        else:
            snap = snap1

        logit.log("stagedfiles.next(): take_snaphot returns %s " % snap)

        self.cs.cs_last_split = snap

        return res

    def len(self):
        return self.samhandle.count_files(self.cs.experiment,"defname:"+self.ds) / self.n + 1
        return res

    def edit_popup(self):
        return "null"
