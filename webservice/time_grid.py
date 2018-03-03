from datetime import datetime
from .utc import utc
import collections

from collections import deque

class time_grid:

    def __init__(self):
        # you can't see boxes less than 2% wide...
        self.minwidth = 2


    def render_query_blob(self, tmin, tmax, rows, group_key, url_template="", extramap={}):
        dlmap = self.group_time_data(rows, group_key, url_template)
        #print "got dlmap:", dlmap
        self.add_time_data(tmin, tmax, dlmap)
        #print "self.pmap is: ", self.pmap
        return self.blobdata(extramap)


    def group_time_data(self, rows, group_key, url_template=""):
        result = collections.OrderedDict()
        lastkey = None
        for row in rows:
            key = getattr(row, group_key)
            if key != lastkey:
                result[key] = deque()
                lastkey = key
            result[key].append({'time': row.created,
                                'status': row.status,
                                'txt':  "%s@%s: %s" % (key, row.created, row.status),
                                'url':  (url_template % row.__dict__) if url_template else getattr(row, 'url', '')
                                })
        return result


    def key(self, fancy=0):
        res = ['<div class="ui raised padded container segment" style="height: %sem">' % (11 if fancy else 5)]
        list = ["new", "Idle", "Held", "Running", "Completed", "Removed", "Located","Failed"]
        if fancy:
            list += ["running: user code",
                     "running: copying files in",
                     "running: copying files out",
                     "running: user code completed",
                     "running: user code succeeded",
                     "running: user code failed"]

        for s in list:
            res.append("""<div style='float:left; margin: 2px; width:16em; background-color: %s;'>
                            <!-- <span style='min-width:4em; background: %%s;'>&nbsp;&nbsp;&nbsp;&nbsp;</span> -->
                            <span style='padding:0 ; float:left; width:13.5em; background-color: #ffffff;'>%s:</span>
                            <span style='float:right; background-color: #ffffff; font-family:monospace;'>&nbsp;&nbsp;&nbsp;</span>
                          </div>"""
                       % (self.status_color(s), s))
        res.append('</div>')
        return '\n'.join(res)


    def status_color(self, line):
        if line.find("started") >= 0:
            return "#303030"
        if line.find("Finished") >= 0:
            return "#ffffff"
        if line.find("Idle") >= 0:
            # return "#808080"
            return "#909090"
        if line.find("new") >= 0:
            # return "#035533"
            return "#0388FF"
        if line.find("Started") >= 0:
            return "#335533"
        if line.find("UserProcessStarted") >= 0:
            return "#335533"
        if line.find("UserExecutable") >= 0:
            return "#11ff11"
        if line.find("Starting") >= 0:
            return "#11ff11"
        if line.find("Held") >= 0:
            return "#ee2211"
        if line.find("running") >= 0:
            if line.find("copying files in") >= 0:
                return "#80f080"
            if line.find("copying files out") >= 0:
                return "#00c0b0"
            if line.find("user code completed") >= 0:
                return "#208010"
            if line.find("user code succeeded") >= 0:
                return "#208010"
            if line.find("user code failed") >= 0:
                return "#901010"
            if line.find("user code") >= 0:
                return "#118811"
            return "#11ff11"
        if line.find("Running") >= 0:
            return "#11ff11"
        if line.find("FileTransfer") >= 0:
            return "#ddffdd"
        if line.find("ifdh::cp") >= 0:
            return "#ddffdd"
        if line.find("idle") >= 0:
            # return "#888888"
            return "#909090"
        if line.find("Completed") >= 0:
            return "#f0f0f0"
        if line.find("Located") >= 0:
            return "#f8f8f8"
        if line.find("Failed") >= 0:
            return "#f80000"
        return "#ffffff"


    def pwidth(self, t0, t1, tmin, tmax):
        if t0 < tmin:
            t0 = tmin
        if t1 > tmax:
            t1 = tmax
        d1 = (t1 - t0)
        d2 = (tmax - tmin)
        pw = d1.total_seconds() * 100.0 / d2.total_seconds()
        return pw


    def add_time_data(self, tmin, tmax, dlistmap):
        self.tmin = tmin
        self.tmax = tmax
        self.tdelta = tmax - tmin
        self.pmap = collections.OrderedDict()
        justnow = datetime.now(utc)
        for id, dlist in list(dlistmap.items()):
            totwidth = 0.0
            plist = deque()
            # somehow this is is never false?!?
            # if dlist[0]['time'] > self.tmin:
            # so compare second conversions...
            if int(dlist[0]['time'].strftime("%s")) > int(self.tmin.strftime("%s")):
                width = self.pwidth(self.tmin, dlist[0]['time'], tmin, tmax)
                plist.append({'width': width, 'color': '', 'txt': '', 'url': ''})
                totwidth += max(width, 0.04)
                i = 0
            else:
                i = 0
                while i < len(dlist) and dlist[i]['time'] < self.tmin:
                    i += 1
                if i < len(dlist):
                    width = self.pwidth(self.tmin, dlist[i]['time'], tmin, tmax)
                    plist.append({'width': width,
                                  'color': self.status_color(dlist[i - 1]['status']),
                                  'txt': dlist[i - 1]['txt'],
                                  'url': dlist[i - 1]['url']})
                    totwidth = totwidth + max(width, 0.04)

            while i < len(dlist) and dlist[i]['time'] <= self.tmax:
                if i == len(dlist) - 1:
                    # last item in row special case...
                    # don't draw boxes past current time...
                    if self.tmax > justnow:
                        tend = justnow
                        width = self.pwidth(dlist[i]['time'], tend, tmin, tmax)
                    else:
                        # take the remaining width, rather than computing
                        # the delta to reduce wraparound
                        tend = self.tmax
                        width = 98.0 - totwidth
                else:
                    tend = dlist[i + 1]['time']
                    width = self.pwidth(dlist[i]['time'], tend, tmin, tmax)

                plist.append({'width': width,
                              'color': self.status_color(dlist[i]['status']),
                              'txt': dlist[i]['txt'],
                              'url': dlist[i]['url']})
                totwidth += max(width, 0.04)
                i += 1

            self.pmap[id] = plist


    def min_box_sizes(self):
        '''
        make sure all boxes are at least min box size
        this makes the large boxes smaller to not
        overflow the row
        '''
        for id, plist in list(self.pmap.items()):
            n_items = 0
            n_too_small = 0
            fudge = 0.0
            for p in plist:
                n_items += 1
                if p['width'] < self.minwidth:
                    n_too_small += 1
                    fudge += self.minwidth - p['width']

            delta = fudge / (n_items - n_too_small)

            for p in plist:
                if p['width'] <= self.minwidth:
                    p['width'] = self.minwidth
                else:
                    if fudge < delta:
                        delta = fudge
                    p['width'] -= delta
                    fudge -= delta

                if p['width'] >= 100:
                    p['width'] = 95


    def blobdata(self, extramap={}):
        blob = {"pmap": self.pmap, "extramap": extramap}
        return blob


if __name__ == '__main__':

    tg = time_grid()

    class fakerow:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    print('<hr>')
    testrows = [
        fakerow(jobid='job1', created=datetime(2016, 2, 29, 13, 1, 0, 0, utc), status="Idle"),
        fakerow(jobid='job1', created=datetime(2016, 2, 29, 13, 6, 0, 0, utc), status="ifdh::cp whatever"),
        fakerow(jobid='job1', created=datetime(2016, 2, 29, 13, 3, 0, 0, utc), status="Running"),
        fakerow(jobid='job1', created=datetime(2016, 2, 29, 13, 9, 0, 0, utc), status="Completed"),
        fakerow(jobid='job2', created=datetime(2016, 2, 29, 13, 2, 0, 0, utc), status="Idle"),
        fakerow(jobid='job2', created=datetime(2016, 2, 29, 13, 4, 0, 0, utc), status="Running"),
        fakerow(jobid='job2', created=datetime(2016, 2, 29, 13, 6, 0, 0, utc), status="ifdh::cp whatever"),
        fakerow(jobid='job2', created=datetime(2016, 2, 29, 13, 10, 0, 0, utc), status="Completed"),
        fakerow(jobid='job3', created=datetime(2016, 2, 29, 13, 2, 0, 0, utc), status="Idle"),
        fakerow(jobid='job3', created=datetime(2016, 2, 29, 13, 4, 0, 0, utc), status="Running"),
        fakerow(jobid='job3', created=datetime(2016, 2, 29, 13, 7, 0, 0, utc), status="ifdh::cp whatever"),
        fakerow(jobid='job3', created=datetime(2016, 2, 29, 13, 10, 0, 0, utc), status="Completed"),
        fakerow(jobid='job4', created=datetime(2016, 2, 29, 13, 3, 0, 0, utc), status="Idle"),
        fakerow(jobid='job4', created=datetime(2016, 2, 29, 13, 5, 0, 0, utc), status="Running"),
        fakerow(jobid='job4', created=datetime(2016, 2, 29, 13, 9, 0, 0, utc), status="ifdh::cp whatever"),
        fakerow(jobid='job4', created=datetime(2016, 2, 29, 13, 11, 0, 0, utc), status="Completed"),
    ]
    print(tg.render_query_blob(datetime(2016, 2, 29, 13, 0, 0, 0, utc), datetime(2016, 2, 29, 13, 30, 0, 0, utc), testrows, 'jobid'))
