from datetime import datetime
from utc import utc

class time_grid:
     
     def __init__(self):
         # you can't see boxes less than 2% wide...
         self.minwidth = 2

     def render_query(self, tmin, tmax, rows, group_key, url_template="", extramap={}):
         dlmap = self.group_time_data( rows, group_key, url_template)
         #print "got dlmap:", dlmap
         self.add_time_data(tmin, tmax, dlmap)
         #print "self.pmap is: ", self.pmap
         return self.draw_boxes(extramap)

     def render_query_blob(self, tmin, tmax, rows, group_key, url_template="", extramap={}):
         dlmap = self.group_time_data( rows, group_key, url_template)
         #print "got dlmap:", dlmap
         self.add_time_data(tmin, tmax, dlmap)
         #print "self.pmap is: ", self.pmap
         return self.blobdata(extramap)

     def group_time_data( self, rows, group_key, url_template="" ):
          result = {}
          lastkey = None
          for row in rows:
              key = getattr(row, group_key)
              if key != lastkey:
                  result[key] = []
                  lastkey = key
              result[key].append( {'time':row.created, 
                                   'status': row.status, 
                                   'txt':  "%s@%s: %s" % (key, row.created, row.status),
                                   'url':  (url_template % row.__dict__) if url_template else getattr(row, 'url', '') 
                                  })
          return result

     def key(self, fancy = 0):
          res = ['<div class="ui raised padded container segment" style="height: %sem">' % (7 if fancy else 5)]
          list = [ "new", "Idle", "Held", "Running", "Completed", "Located"]
          if fancy:
                list = list + ["running: user code",
              		"running: copying files in",
              		"running: copying files out",
              		"running: user code completed",
              		"running: user code succeeded",
              		"running: user code failed"]

          for s in list:
              res.append( "<div style='float:left; width:15em'><span style='min-width:3em; background: %s'>&nbsp;&nbsp;</span>%s</div>" % (self.status_color(s), s))
          res.append('</div>')
          return '\n'.join(res)
             

     def status_color(self,str):
          if str.find("started") >= 0:
              return "#303030"
          if str.find("Finished") >= 0:
              return "#ffffff"
          if str.find("Idle") >= 0:
              return "#808080"
          if str.find("new") >= 0:
              return "#035533"
          if str.find("Started") >= 0:
              return "#335533"
          if str.find("UserProcessStarted") >= 0:
              return "#335533"
          if str.find("UserExecutable") >= 0:
              return "#11ff11"
          if str.find("Starting") >= 0:
              return "#11ff11"
          if str.find("Held") >= 0:
              return "#ee2211"
          if str.find("running") >= 0:
              if str.find("user code") >= 0:
                  return "#118811"
              if str.find("copying files in") >= 0:
                  return "#80f080"
              if str.find("copying files out") >= 0:
                  return "#00c0b0"
              if str.find("user code completed") >= 0:
                  return "#208010"
              if str.find("user code succeeded") >= 0:
                  return "#20e010"
              if str.find("user code failed") >= 0:
                  return "#e01010"
              return "#11ff11"
          if str.find("Running") >= 0:
              return "#11ff11"
          if str.find("FileTransfer") >= 0:
              return "#ddffdd"
          if str.find("ifdh::cp") >= 0:
              return "#ddffdd"
          if str.find("idle") >= 0:
              return "#888888"
          if str.find("Completed") >= 0:
              return "#f0f0f0"
          if str.find("Located") >= 0:
              return "#f8f8f8"
          return "#ffffff"

     def pwidth(self, t0, t1, tmin, tmax):
          if t0 < tmin:
             t0 = tmin
          if t1 > tmax:
             t1 = tmax
          d1 = (t1 - t0)
          d2 = (tmax - tmin)
          pw = d1.total_seconds() * 99.9 / d2.total_seconds()
          return pw

     def add_time_data(self, tmin, tmax, dlistmap):
          self.tmin = tmin
          self.tmax = tmax
          self.tdelta = tmax - tmin
          self.pmap = {}
          justnow = datetime.now(utc)
          for id,dlist in dlistmap.items():
              plist = []
              # somehow this is is never false?!?
              # if dlist[0]['time'] > self.tmin:
              # so compare second conversions...
              if int(dlist[0]['time'].strftime("%s")) > int(self.tmin.strftime("%s")):
                  plist.append( {'width': self.pwidth(self.tmin, dlist[0]['time'],tmin, tmax),
                             'color': '', 'txt': '', 'url': ''})
                  i = 0
              else:
                  i = 0 
                  while i < len(dlist) and dlist[i]['time'] < self.tmin:
                     i = i + 1
                  if i < len(dlist):
                      plist.append({ 'width': self.pwidth(self.tmin, dlist[i]['time'],tmin, tmax),
                                'color': self.status_color(dlist[i-1]['status']),
                                'txt': dlist[i-1]['txt'],
                                'url': dlist[i-1]['url']})

              while i < len(dlist) and dlist[i]['time'] < self.tmax:
                  if i == len(dlist) - 1:
                      # don't draw boxes past current time...
                      if justnow > self.tmin:
                          tend = min(self.tmax, justnow)
                      else:
                          tend = self.tmax
                  else:
                      tend = dlist[i+1]['time']

                  plist.append({ 'width' : self.pwidth(dlist[i]['time'], tend, tmin,tmax),
                                'color' : self.status_color(dlist[i]['status']),
                                'txt' : dlist[i]['txt'],
                                'url' : dlist[i]['url']})
                  i = i + 1
                 
              self.pmap[id] = plist

     def min_box_sizes(self):
         '''
             make sure all boxes are at least min box size
             this makes the large boxes smaller to not
             overflow the row
         '''
         for id,plist in self.pmap.items():
             n_items=0
             n_too_small=0
             fudge = 0.0
             for p in plist:
                 n_items = n_items + 1
                 if p['width'] < self.minwidth:
                    n_too_small = n_too_small + 1
                    fudge = fudge + self.minwidth - p['width']

             delta = fudge / (n_items - n_too_small)

             for p in plist:
                 if p['width'] <= self.minwidth:
                     p['width'] = self.minwidth
                 else:
                     if fudge < delta:
                        delta = fudge
                     p['width'] = p['width'] - delta
                     fudge = fudge - delta

                 if p['width'] >= 100:
                    p['width'] = 95
             
     def blobdata(self, extramap = {}):
         blob = {"pmap": self.pmap, "extramap": extramap}
         return blob

     def draw_boxes(self, extramap = {}):
         rlist = []
         displaylist = self.pmap.items()
         displaylist.sort(key=lambda x: x[0])
         for id,plist in displaylist:
             if len(plist) == 0:
                 continue
             rlist.append("""
               <div class='row' style='margin:0px 0px; padding:0px 0px;'>
                 <div class='mwm_label' style='text-align: right; width: 14%%; float:left; padding: 5px 5px; border-right: 1px solid black; '>%s</div>
                   <div clas='mwm_rowdata'  style='position: relative; width: 85%%; float:left; clear: right; border-right: 1px solid black; border-left: 1px solid black; padding: 5px 0px'>
                """ % (id + extramap.get(id,''))
)
             for p in plist:
                 rlist.append("""
                       <a href='%s'>
                         <div class='tbox' data-content='%s' data-variation="very wide" style='width: %f%%; background-color: %s !important; float:left; '>
                           &nbsp;
                         </div>
                       </a>
                   """ % ( p['url'], p['txt'], p['width'], p['color']))
             rlist.append("""
                    </div>
                    &nbsp;
               </div>
                 """)
         return "\n".join(rlist)

if __name__ == '__main__':

    tg = time_grid()

    class fakerow:
        def __init__(self, **kwargs):
             self.__dict__.update(kwargs)

    print '<hr>'
    testrows = [ 
         fakerow( jobid= 'job1', created = datetime(2016,2,29,13,1,0,0,utc), status = "Idle"),
         fakerow( jobid= 'job1', created = datetime(2016,2,29,13,6,0,0,utc), status = "ifdh::cp whatever"),
         fakerow( jobid= 'job1', created = datetime(2016,2,29,13,3,0,0,utc), status = "Running"),
         fakerow( jobid= 'job1', created = datetime(2016,2,29,13,9,0,0,utc), status = "Completed"),
         fakerow( jobid= 'job2', created = datetime(2016,2,29,13,2,0,0,utc), status = "Idle"),
         fakerow( jobid= 'job2', created = datetime(2016,2,29,13,4,0,0,utc), status = "Running"),
         fakerow( jobid= 'job2', created = datetime(2016,2,29,13,6,0,0,utc), status = "ifdh::cp whatever"),
         fakerow( jobid= 'job2', created = datetime(2016,2,29,13,10,0,0,utc), status = "Completed"),
         fakerow( jobid= 'job3', created = datetime(2016,2,29,13,2,0,0,utc), status = "Idle"),
         fakerow( jobid= 'job3', created = datetime(2016,2,29,13,4,0,0,utc), status = "Running"),
         fakerow( jobid= 'job3', created = datetime(2016,2,29,13,7,0,0,utc), status = "ifdh::cp whatever"),
         fakerow( jobid= 'job3', created = datetime(2016,2,29,13,10,0,0,utc), status = "Completed"),
         fakerow( jobid= 'job4', created = datetime(2016,2,29,13,3,0,0,utc), status = "Idle"),
         fakerow( jobid= 'job4', created = datetime(2016,2,29,13,5,0,0,utc), status = "Running"),
         fakerow( jobid= 'job4', created = datetime(2016,2,29,13,9,0,0,utc), status = "ifdh::cp whatever"),
         fakerow( jobid= 'job4', created = datetime(2016,2,29,13,11,0,0,utc), status = "Completed"),
        ]
    print tg.render_query( datetime(2016,2,29,13,0,0,0,utc), datetime(2016,2,29,13,30,0,0,utc), testrows,'jobid')
