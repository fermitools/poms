import uuid
class byrun:
    """
       This type, when filled out as byrun(low=2,high=4) will 
       slice the dataset into parts by picking run numbers 2..4
       one run per batch. Bug:  It does not handle empty runs well
    """

    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.ds = cs.dataset
        self.low = 1
        self.high = 999999
        self.id = uuid.uuid4()
        parms = cs.cs_split_type[6:].split(",")
        low = 1
        for p in parms:
            if p.endswith(")"):
                p = p[:-1]
            if p.startswith("low="):
                self.low = int(p[4:])
            if p.startswith("high="):
                self.high = int(p[5:])

        self.samhandle = samhandle

    def params(self):
        return ["low=", "high="]

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = self.low
        if self.cs.cs_last_split >= self.high:
            raise StopIteration

        new = self.cs.dataset + "_%s_run_%d" % (str(self.id),self.cs.cs_last_split)
        self.samhandle.create_definition(
            self.cs.job_type_obj.experiment, new, "defname: %s and run_number %d" % (self.ds, self.cs.cs_last_split)
        )
        return new

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split + 1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split - 1
        res = self.peek()
        return res

    def len(self):
        return self.high - self.low + 1

    def edit_popup(self):
        return "null"
