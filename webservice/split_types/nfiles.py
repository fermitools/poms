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
        self.n = int(camp.cs_split_type[7:].strip(')'))
        return c.dataset

    def peek(self):
        if not self.c.cs_last_split:
            self.c.cs_last_split = 0

        new = camp.dataset + "_slice%d" % (camp.cs_last_split, self.n)
        self.samhandle.create_definition(camp.campaign_definition_obj.experiment, new,  "defname: %s with limit %d offset %d" % (camp.dataset, self.n, camp.cs_last_split * self.n))
        if samhandle.count_files(self.c.campaign_definition_obj.experiment, new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        self.c.cs_last_split = self.c.cs_last_split+1

    def len(self):
        return self.samhandle.count_files(self.c.campaign_definition_obj.experiment,self.ds) / self.n + 1
