class nfiles:
    """
       This type, when filled out as nfiles(n) or nfiles_n for some integer
       n, will slice the dataset into n parts using the stride/offset
       expressions.  This does not work so well for dynamic datasets whose
       contents are changing, for them try "drainingn"
    """
    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.ds = cs.dataset
        try:
            self.n = int(cs.cs_split_type[7:].strip(')'))
        except:
            raise SyntaxError("unable to parse integer parameter from '%s'" % cs.cs_split_type)

    def params(self):
        return ["n"]

    def peek(self):
        if not self.cs.cs_last_split:
            self.cs.cs_last_split = 0

        new = self.cs.dataset + "_slice%d_files%d" % (self.cs.cs_last_split, self.n)
        self.samhandle.create_definition(self.cs.experiment, new,  "defname: %s with limit %d offset %d" % (self.cs.dataset, self.n, self.cs.cs_last_split * self.n))
        if self.samhandle.count_files(self.cs.job_type_obj.experiment, "defname:"+new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split+1
        return res

    def len(self):
        return self.samhandle.count_files(self.cs.experiment,"defname:"+self.ds) / self.n + 1
        return res

    def edit_popup(self):
        return "null"
