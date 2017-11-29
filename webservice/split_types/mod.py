class mod:
    """
       This type, when filled out as mod(n) or mod_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.
    """
    def __init__(self, c, samhandle, dbhandle):
        self.c = c
        self.ds = c.dataset
        self.m = int(c.cs_split_type[4:].strip(')'))
        self.samhandle = samhandle

    def peek(self):
        if not self.c.cs_last_split:
            self.c.cs_last_split = 0
        if self.c.cs_last_split >= self.m:
            raise StopIteration

        new = self.c.dataset + "_slice%d_of_%d" % (self.c.cs_last_split, self.m)
        self.samhandle.create_definition(self.c.campaign_definition_obj.experiment, new,  "defname: %s with stride %d offset %d" % (self.c.dataset, self.m, self.c.cs_last_split))
        return new

    def next(self):
        res = self.peek()
        self.c.cs_last_split = self.c.cs_last_split+1
        return res

    def len(self):
        return self.m
