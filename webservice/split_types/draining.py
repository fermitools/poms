import uuid
class draining:
    """
       This type just always returns the same dataset name forever
       assuming you have a draining/recursive definition that gives
       you the work remaining to be done.
    """

    def __init__(self, cs, samhandle, dbhandle, test=False):
        self.test = test
        self.only = cs.dataset
        self.id = uuid.uuid4()

    def params(self):
        return []

    def peek(self):
        return "%s" % (self.only)

    def next(self):
        res = self.peek()
        return res

    def prev(self):
        res = self.peek()
        return res

    def len(self):
        return -1

    def edit_popup(self):
        return "null"
