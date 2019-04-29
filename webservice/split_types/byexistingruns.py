import poms.webservice.logit as logit


class byexistingruns:
    """
       This type, when filled out as byexistingruns() 
       will pick a run from the unprocessed files, and make
       a batch out of all files of that run each time,
       tracking the processed files in a snapshot whose id
       is stored in cs_last_split
    """

    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.dataset = cs.dataset

    def params(self):
        return []

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
            snapshotbit = ""
        else:
            snapshotbit = "minus snapshot_id %d" % self.cs.cs_last_split

        filel = self.samhandle.plain_list_files(self.cs.experiment, "defname:%s %s with limit 1" % (self.dataset, snapshotbit))
        if len(filel) == 0:
            raise StopIteration

        md = self.samhandle.get_metadata(self.cs.experiment, filel[0])
        if not md.get("runs", None):
            raise StopIteration

        run_number = "%d.%04d" % (md["runs"][0][0], md["runs"][0][1])
        new = self.cs.dataset + "_slice_%i_run_%s" % (self.cs.cs_last_split, run_number)

        self.samhandle.create_definition(
            self.cs.experiment, new, "defname: %s  %s and run_number %s " % (self.cs.dataset, snapshotbit, run_number)
        )

        return new

    def next(self):
        res = self.peek()
        newfullname = res.replace("slice", "full")
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
