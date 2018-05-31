
class list:
    """
       This split type assumes you have been given a comma-separated list 
       of dataset names to work through in the dataset field, and will
       submit each one separately
    """
    def __init__(self, cs, samhandle, dbhandle):
        self.cs = cs
        self.list = cs.dataset.split(',')

    def peek(self):
        if self.cs.cs_last_split == None:
            self.cs.cs_last_split = 0
        if self.cs.cs_last_split >= len(self.list):
            raise StopIteration
        return self.list[self.cs.cs_last_split]

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.cs.cs_last_split+1
        return res

    def prev(self):
        self.cs.cs_last_split = self.cs.cs_last_split-1
        res = self.peek()
        return res

    def len(self):
        return len(self.list)
