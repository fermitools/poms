class byrun:
    """
       This type, when filled out as mod(n) or mod_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.
    """
    def __init__(self, c, samhandle, dbhandle):
        self.c = c
        self.ds = c.dataset
        self.low = 1
        self.high = 999999
        parms = c.cs_split_type[6:].split(',')
        low = 1
        for p in parms:
            if p.endswith(')'): p = p[:-1]
            if p.startswith('low='): self.low = int(p[4:])
            if p.startswith('high='): self.high = int(p[5:])

        self.samhandle = samhandle

    def peek(self):
        if not self.c.cs_last_split:
            self.c.cs_last_split = self.low
        if self.c.cs_last_split >= self.high:
            raise StopIteration

        new = self.c.dataset + "_run_%d" % (self.c.cs_last_split)
        self.samhandle.create_definition(self.c.campaign_definition_obj.experiment, new,  "defname: %s and run_number %d" % (self.ds,  self.c.cs_last_split))
        return new

    def next(self):
        res = self.peek()
        self.c.cs_last_split = self.c.cs_last_split+1
        return res

    def prev(self):
        self.c.cs_last_split = self.c.cs_last_split-1
        res = self.peek()
        return res

    def len(self):
        return self.high - self.low + 1
