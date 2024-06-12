import uuid
class limitn:
    """
       This type, when filled out as limitn(n)  for some integer
       n, will just make a datset of the  "defname:base with limit n"
       Useful for analysis users who want a limited set
    """

    def __init__(self, cs, samhandle, dbhandle, test=False):
        self.test = test
        self.cs = cs
        self.samhandle = samhandle
        self.dbhandle = dbhandle
        self.ds = cs.dataset
        self.id = uuid.uuid4()
        try:
            self.n = int(cs.cs_split_type[7:].strip(")")) if not self.test else int(cs.test_split_type[7:].strip(")"))
        except:
            raise SyntaxError("unable to parse integer parameter from '%s'" % cs.cs_split_type if not self.test else cs.test_split_type)

    def params(self):
        return ["n"]

    def peek(self):
        new = self.cs.dataset + "_%s_limit%d" % (str(self.id), self.n)
        self.samhandle.create_definition(self.cs.experiment, new, "defname: %s with limit %d" % (self.cs.dataset, self.n))
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
