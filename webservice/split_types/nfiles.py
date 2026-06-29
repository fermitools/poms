import uuid
class nfiles:
    """
       This type, when filled out as nfiles(n) or nfiles_n for some integer
       n, will slice the dataset into parts of n files using the stride/offset
       expressions.  This does not work so well for dynamic datasets whose
       contents are changing, for them try "drainingn"
    """

    def __init__(self, cs, samhandle, dbhandle, test=False):
        self.test = test
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.ds = cs.dataset
        try:
            self.n = int(cs.cs_split_type[7:].strip(")")) if not self.test else int(cs.test_split_type[7:].strip(")"))
        except:
            raise SyntaxError("unable to parse integer parameter from '%s'" % cs.cs_split_type if not self.test else cs.test_split_type)

    def params(self):
        return ["n"]

    def peek(self):
        if self.test:
            ls = self.cs.last_split_test
        else:
            ls = self.cs.cs_last_split
        if not ls:
            if test:
                self.cs.last_split_test = 0
            else:
                self.cs.cs_last_split = 0
            ls = 0

        new = self.cs.dataset + "_slice%d_files%d" % (ls, self.n)
        self.samhandle.create_definition(
            self.cs.experiment,
            new,
            "defname: %s with limit %d offset %d" % (self.cs.dataset, self.n, ls * self.n),
        )
        if self.samhandle.count_files(self.cs.job_type_obj.experiment, "defname:" + new) == 0:
            raise StopIteration

        return new

    def next(self):
        res = self.peek()
        if self.test:
            self.cs.last_split_test = self.cs.last_split_test + 1
        else:
            self.cs.cs_last_split = self.cs.cs_last_split + 1
        return res

    def len(self):
        return self.samhandle.count_files(self.cs.experiment, "defname:" + self.ds) / self.n + 1
        return res

    def edit_popup(self):
        return "null"
