import uuid

class mod:
    """
       This type, when filled out as mod(n) or mod_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.
    """

    def __init__(self, cs, samhandle, dbhandle, test=False):
        self.test = test
        self.cs = cs
        self.ds = cs.dataset
        self.m = int(cs.cs_split_type[4:].strip(")")) if not self.test else int(cs.test_split_type[4:].strip(")"))
        self.samhandle = samhandle

    def params(self):
        return ["modulus"]

    def peek(self):
        if self.test:
            ls = self.cs.test_last_split
        else:
            ls = self.cs.cs_last_split

        if not ls:
            if self.test:
                self.cs.test_last_split = 0
            else:
                self.cs.cs_last_split = 0
            ls = 0

        if ls >= self.m:
            raise StopIteration

        new = self.ds + "_slice%d_of_%d" % (ls, self.m)
        self.samhandle.create_definition(
            self.cs.job_type_obj.experiment,
            new,
            "defname: %s with stride %d offset %d" % (self.cs.dataset, self.m, ls),
        )
        return new

    def next(self):
        res = self.peek()
        if self.test:
            self.cs.test_last_split = self.cs.test_last_split + 1
        else:
            self.cs.cs_last_split = self.cs.cs_last_split + 1
        return res

    def prev(self):
        if self.test:
            self.cs.test_last_split = self.cs.test_last_split - 1
        else:
            self.cs.cs_last_split = self.cs.cs_last_split - 1
        res = self.peek()
        return res

    def len(self):
        return self.m

    def edit_popup(self):
        return "null"
