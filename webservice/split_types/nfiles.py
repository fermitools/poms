class nfiles:
    """
       This type, when filled out as mod(n) or mod_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.
    """
    def __init__(self, c, samhandle, dbhandle):
        self.c = c
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.ds = c.dataset
        self.n = int(c.cs_split_type[7:].strip(')'))

    def peek(self):
        if not self.c.cs_last_split:
            self.c.cs_last_split = 0

        new = self.c.dataset + "_slice%d_files%d" % (self.c.cs_last_split, self.n)
        self.samhandle.create_definition(self.c.experiment, new,  "defname: %s with limit %d offset %d" % (self.c.dataset, self.n, self.c.cs_last_split * self.n))
        if self.samhandle.count_files(self.c.campaign_definition_obj.experiment, "defname:"+new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        self.c.cs_last_split = self.c.cs_last_split+1
        return res

    def len(self):
        return self.samhandle.count_files(self.c.experiment,"defname:"+self.ds) / self.n + 1
        return res
