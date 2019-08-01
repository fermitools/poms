class limitn:
    """
       This type, when filled out as limitn(n)  for some integer
       n, will just make a datset of the  "defname:base with limit n"
       Useful for analysis users who want a limited set
    """

    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.ds = cs.dataset
        try:
            self.n = int(cs.cs_split_type[7:].strip(")"))
        except:
            raise SyntaxError("unable to parse integer parameter from '%s'" % cs.cs_split_type)

    def params(self):
        return ["n"]

    def peek(self):
        new = self.cs.dataset + "_limit%d" % (self.n)
        self.samhandle.create_definition(
            self.cs.experiment,
            new,
            "defname: %s with limit %d" % (self.cs.dataset, self.n)
        )
        if self.samhandle.count_files(self.cs.job_type_obj.experiment, "defname:" + new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        return res

    def len(self):
        return self.n
        return res

    def edit_popup(self):
        return "null"
