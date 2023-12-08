import time
from datetime import datetime

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
       1497934800: (i.e. from "date -d 2017-06-20 +%s") 
           new(firsttime=1497934800, window=1w)
    """

    def __init__(self, ctx, cs, test=False):
        self.test = test
        self.cs = cs
        self.dmr_service = ctx.dmr_service
        # default parameters
        self.tfts = 1800.0  # half an hour
        self.twindow = 604800.0  # one week
        self.tround = 1  # one second
        self.tlocaltime = 0  # assume GMT
        self.tfirsttime = None  # override start time
        self.tlasttime = time.time()  # override end time -- default now
        if (not test and cs.cs_split_type[3:] == "_local") or (test and cs.test_split_type[3:] == "_local"):
                self.tlocaltime = 1

        # if they specified any, grab them ...
        if (not test and cs.cs_split_type[3] == "(") or (test and cs.test_split_type[3] == "("):
            parms = cs.cs_split_type[4:].split(",") if not test else cs.test_split_type[4:].split(",")
            for p in parms:
                pmult = 1
                if p.endswith(")"):
                    p = p[:-1]
                if p.endswith("w"):
                    pmult = 604800
                    p = p[:-1]
                if p.endswith("d"):
                    pmult = 86400
                    p = p[:-1]
                if p.endswith("h"):
                    pmult = 3600
                    p = p[:-1]
                if p.endswith("m"):
                    pmult = 60
                    p = p[:-1]
                if p.endswith("s"):
                    pmult = 1
                    p = p[:-1]
                if p.startswith("window="):
                    self.twindow = float(p[7:]) * pmult
                if p.startswith("round="):
                    self.tround = float(p[6:]) * pmult
                if p.startswith("fts="):
                    self.tfts = float(p[4:]) * pmult
                if p.startswith("localtime="):
                    self.tlocaltime = float(p[10:]) * pmult
                if p.startswith("firsttime="):
                    self.tfirsttime = float(p[10:]) * pmult
                if p.startswith("lasttime="):
                    self.tlasttime = float(p[10:]) * pmult

        # make sure time-window is a multiple of rounding factor
        self.twindow = int(self.twindow) - (int(self.twindow) % int(self.tround))

    def params(self):
        return ["window=", "round=", "fts=", "localtime=", "firsttime=", "lasttime="]

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
        
        query = "%s and created_timestamp > '%s' and created_timestamp <= '%s'" % (self.cs.data_dispatcher_dataset_query, sstime, setime)
        project_files = list(self.dmr_service.metacat_client.query(query, with_metadata=True))
        project_name = "%s | new | %s -> %s" % (self.cs.name, self.format_time(sstime), self.format_time(setime))

        return self.create_project(project_name, project_files)

    def prev(self):
        self.cs.cs_last_split = self.etime - self.twindow
        res = self.peek()
        return res

    def next(self):
        res = self.peek()
        self.cs.cs_last_split = self.etime
        return res

    def len(self):
        return int((time.time() - self.firsttime) / self.twindow)

    def edit_popup(self):
        return "null"
    
    def format_time(self,input_time):
        parsed_time = datetime.strptime(input_time, "%Y-%m-%dT%H:%M:%S")
        formatted_time = parsed_time.strftime("%Y-%m-%d %I:%M %p")
        return formatted_time
    
    def create_project(self, project_name, project_files):
        dd_project = self.dmr_service.create_project(username=self.cs.experimenter_creator_obj.username, 
                                        files=project_files,
                                        experiment=self.cs.experiment,
                                        role=self.cs.vo_role,
                                        project_name=project_name,
                                        campaign_id=self.cs.campaign_id, 
                                        campaign_stage_id=self.cs.campaign_stage_id,
                                        split_type=self.cs.cs_split_type if not self.test else self.cs.test_split_type,
                                        last_split=self.cs.cs_last_split,
                                        creator=self.cs.experimenter_creator_obj.experimenter_id,
                                        creator_name=self.cs.experimenter_creator_obj.username)
        return dd_project
