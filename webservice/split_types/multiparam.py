import json

class multiparam:
    """
       This split type assumes you have been given a list of lists of 
       strings, and returns a string of the nth combination ... for
       example if you have:
        [['a','b','c'],['d','e','f'],['g','h','i']]
        this should give you:
        a_d_g
        b_d_g
        c_d_g
        a_e_g
        b_e_g
        ...
    """
    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.list = json.loads(cs.dataset)
        self.dims = []
        for l1 in self.list:
            self.dims.append(len(l1))

    def peek(self):
        if self.cs.cs_last_split == None:
            self.cs.cs_last_split = 0
        if self.cs.cs_last_split >= self.len():
            raise StopIteration
        n = self.cs.cs_last_split 
        res = []
        i = 0
        for l1 in self.list:
            res.append(l1[n % dims[i]])
            n = n / dims[i]
        return '_'.join(res)

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split+1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split-1
        res = self.peek()
        return res

    def len(self):
        c = 1
        for l1 in self.list:
            c = c * len(l1)
