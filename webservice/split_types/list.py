
class list:
    """
       This split type assumes you have been given a comma-separated list 
       of dataset names to work through in the dataset field, and will
       submit each one separately
    """
    def __init__(self, c, samhandle, dbhandle):
        self.c = c
        self.list = c.dataset.split(',')

    def peek(self):
        if self.c.cs_last_split == None:
            self.c.cs_last_split = 0
        if self.c.cs_last_split >= len(self.list):
            raise StopIteration
        return self.list[self.c.cs_last_split]

    def next(self):
        res = self.peek()
        self.c.cs_last_split = self.c.cs_last_split+1

    def len(self):
        return len(self.list)
