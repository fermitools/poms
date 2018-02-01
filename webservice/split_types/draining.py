
class draining:
    """
       This type just always returns the same dataset name forever
       assuming you have a draining/recursive definition that gives
       you the work remaining to be done.
    """
    def __init__(self, c, samhandle, dbhandle):
        self.only = c.dataset

    def peek(self):
        return self.only

    def next(self):
        res = self.peek()
        return res

    def prev(self):
        res = self.peek()
        return res

    def len(self):
        return -1
