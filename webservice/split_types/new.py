
import time

class new:
    """
       This is invoked as:
         new(param=val, param=val ...) or 
         new_local(param=val, param=val ...)
       will do files new since the last time recorded with params/modifiers
       params are:
            window=     time window 
            round=      round times to nearest
            fts=        time to assume fts takes to register new files
            localtime=  whether to use localtime or gmt
            firsttime=  first time to start with
            lasttime=   stop when you hit this time
       all values can have suffixes from {wdhms} for week, day, hour, minutes,
       seconds.
       So if you just want to do new files, but at most 6 hours at at a time:
           new(window=6h)
       If you want catch up and to do one-week chunks starting at unix time 
       1497934800: (i.e. from "date -D 2017-06-20 +%s") 
           new(firsttime=1497934800, window=1w)
    """
    def __init__(self, camp, samhandle, dbhandle):
        self.cs = camp
        self.samhandle = samhandle
        # default parameters
        self.tfts = 1800.0 # half an hour
        self.twindow = 604800.0     # one week
        self.tround = 1             # one second
        self.tlocaltime = 0         # assume GMT
        self.tfirsttime = None      # override start time
        self.tlasttime = time.time()# override end time -- default now 

        if camp.cs_split_type[3:] == '_local':
            self.tlocaltime = 1

        # if they specified any, grab them ...
        if camp.cs_split_type[3] == '(':
            parms = camp.cs_split_type[4:].split(',')
            for p in parms:
                pmult = 1
                if p.endswith(')'): p=p[:-1]
                if p.endswith('w'): pmult = 604800; p=p[:-1]
                if p.endswith('d'): pmult = 86400; p=p[:-1]
                if p.endswith('h'): pmult = 3600; p=p[:-1]
                if p.endswith('m'): pmult = 60; p=p[:-1]
                if p.endswith('s'): pmult = 1; p=p[:-1]
                if p.startswith('window='): self.twindow = float(p[7:]) * pmult
                if p.startswith('round='): self.tround = float(p[6:]) * pmult
                if p.startswith('fts='): self.tfts = float(p[4:]) * pmult
                if p.startswith('localtime='): self.tlocaltime = float(p[10:]) * pmult
                if p.startswith('firsttime='): self.tfirsttime = float(p[10:]) * pmult
                if p.startswith('lasttime='): self.tlasttime = float(p[10:]) * pmult

        # make sure time-window is a multiple of rounding factor
        self.twindow = int(self.twindow) - (int(self.twindow) % int(self.tround))
    def params(self):
        return [
            "window=",
            "round=",
            "fts=",
            "localtime=",
            "firsttime=",
            "lasttime=",
         ]
    def peek(self):
        bound_time = self.tlasttime - self.tfts - self.twindow
        bound_time = int(bound_time) - (int(bound_time) % int(self.tround))

        if not self.cs.cs_last_split:
            if self.tfirsttime:
                stime = self.tfirsttime
            else:
                stime = bound_time
        else:
            if self.cs.cs_last_split > bound_time:
                raise StopIteration
            stime = self.cs.cs_last_split

        etime = stime + self.twindow

        if self.tlocaltime:
            sstime = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(stime))
            setime = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(etime))
        else:
            sstime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(stime))
            setime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(etime))

        self.etime = etime

        new = self.cs.dataset + "_time_%s__%s" % (int(stime), int(etime))

        self.samhandle.create_definition(
                self.cs.job_type_obj.experiment,
                new,
                "defname: %s and end_time > '%s' and end_time <= '%s'" % (
                      self.cs.dataset, sstime, setime
                )
            )

        return new


    def prev(self):
        self.cs.cs_last_split = self.etime - self.twindow
        res = self.peek()
        return res

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.etime
        return res

    def len(self):
        return int((time.time() - self.firsttime)/self.twindow)

    def edit_popup(self):
        return "null"
