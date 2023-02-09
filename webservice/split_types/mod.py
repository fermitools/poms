import uuid

class mod:
    """
       This type, when filled out as mod(n) or mod_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.
    """

    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.ds = cs.dataset
        self.m = int(cs.cs_split_type[4:].strip(")"))
        self.samhandle = samhandle

    def params(self):
        return ["modulus"]

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0
        if self.cs.cs_last_split >= self.m:
            raise StopIteration

        new = self.ds + "_slice%d_of_%d" % (self.cs.cs_last_split, self.m)
        self.samhandle.create_definition(
            self.cs.job_type_obj.experiment,
            new,
            "defname: %s with stride %d offset %d" % (self.cs.dataset, self.m, self.cs.cs_last_split),
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
        return self.m

    def edit_popup(self):
        return "null"
