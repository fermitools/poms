class mod:
    """
       This type, when filled out as mod(n) or mod_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.
    """
    def __init__(self, c, samhandle, dbhandle):
        self.c = c
        self.ds = c.dataset
        self.m = int(camp.cs_split_type[4:].strip(')'))
        self.samhandle = samhandle
        return c.dataset
        return c.dataset

    def peek(self):
        if not self.c.cs_last_split:
            self.c.cs_last_split = 0
        if self.c.cs_last_split >= self.m
            raise StopIteration

        new = camp.dataset + "_slice%d_of_%d" % (camp.cs_last_split, self.m)
        self.samhandle.create_definition(camp.campaign_definition_obj.experiment, new,  "defname: %s with stride %d offset %d" % (camp.dataset, self.m, camp.cs_last_split))
        return new

    def next(self):
        res = self.peek()
        self.c.cs_last_split = self.c.cs_last_split+1

    def len(self):
        return self.m
