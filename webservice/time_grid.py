class time_grid:
     
     def __init__(self):
         # you can't see boxes less than 4% wide...
         self.minwidth = 4

     def render_query(self, tmin, tmax, rows, group_key, url_template=""):
         dlmap = self.group_time_data( rows, group_key, url_template)
         #print "got dlmap:", dlmap
         self.add_time_data(tmin, tmax, dlmap)
         #print "self.pmap is: ", self.pmap
         return self.draw_boxes()

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

     def status_color(self,str):
          if str.find("Finished") >= 0:
              return "#ffffff"
          if str.find("Idle") >= 0:
              return "#808080"
          if str.find("new") >= 0:
              return "#035533"
          if str.find("started") >= 0:
              return "#335533"
          if str.find("Started") >= 0:
              return "#335533"
          if str.find("UserProcessStarted") >= 0:
              return "#335533"
          if str.find("UserExecutable") >= 0:
              return "#11ff11"
          if str.find("Starting") >= 0:
              return "#11ff11"
          if str.find("running") >= 0:
              return "#11ff11"
          if str.find("Running") >= 0:
              return "#11ff11"
          if str.find("FileTransfer") >= 0:
              return "#ddffdd"
          if str.find("ifdh::cp") >= 0:
              return "#ddffdd"
          if str.find("idle") >= 0:
              return "#888888"
          return "#ffffff"

     def pwidth(self, t0, t1):
          return (t1 - t0).total_seconds() * 99 / (self.tdelta.total_seconds())

     def add_time_data(self, tmin, tmax, dlistmap):
          self.tmin = tmin
          self.tmax = tmax
          self.tdelta = tmax - tmin
          self.pmap = {}
          for id,dlist in dlistmap.items():
              plist = []
              if dlist[0]['time'] > self.tmin:
                  plist.append( {'width': self.pwidth(self.tmin, dlist[0]['time']),
                             'color': '', 'txt': '', 'url': ''})
                  stime = dlist[0]['time']
                  i = 0
              else:
                  i = 0 
                  while dlist[i]['time'] < self.tmin:
                     i = i + 1
                  plist.append({ 'width': self.pwidth(self.tmin, dlist[i-1]['time']),
                                'color': self.status_color(dlist[i-1].status),
                                'txt': dlist[i-1]['txt'],
                                'url': dlist[i-1]['url']})
              while i < len(dlist) and dlist[i]['time'] < self.tmax:
                  if i == len(dlist) - 1:
                      tend = self.tmax
                  else:
                      tend = dlist[i+1]['time']
                  plist.append({ 'width' : self.pwidth(dlist[i]['time'], tend),
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
             delta = int(fudge / (n_items - n_too_small) + 0.9)
             for p in plist:
                 if p['width'] < self.minwidth:
                     p['width'] = self.minwidth
                 else:
                     if fudge < delta:
                        delta = fudge
                     p['width'] = p['width'] - delta
                     fudge = fudge - delta
             
     def draw_boxes(self):
         self.min_box_sizes()
         rlist = []
         for id,plist in self.pmap.items():
             rlist.append("""
               <div class='row' style='margin:0px 0px; padding:0px 0px;'>
                 <div class='mwm_label' style='text-align: right; width: 9%%; float:left; padding: 5px 5px; border-right: 1px solid black; '>%s</div>
                   <div clas='mwm_rowdata'  style='width: 85%%; float:left; clear: right; border-right: 1px solid black; padding: 5px 0px'>
                """ % id)
             for p in plist:
                 rlist.append("""
                       <a href='%s' title='%s'>
                         <div class='tbox' style='width: %d%%; background-color: %s !important; float:left; '>
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
    tg.add_time_data(1, 12, {
	'a': [  
	       {'time': 2,
		'status': 'idle',
		'url': '',
		'txt': 'idle'},
	       {'time': 4,
		'status': 'running',
		'url': '',
		'txt': 'running'},
	       {'time': 6,
		'status': 'ifdh::cp foo bar',
		'url': '',
		'txt': 'ifdh::cp foo bar'},
	       {'time': 8,
		'status': 'complete',
		'url': '',
		'txt': 'complete'}, 
	    ],
	'b': [  
	       {'time': 3,
		'status': 'idle',
		'url': '',
		'txt': 'idle'},
	       {'time': 5,
		'status': 'running',
		'url': '',
		'txt': 'running'},
	       {'time': 7,
		'status': 'ifdh::cp foo bar',
		'url': '',
		'txt': 'ifdh::cp foo bar'},
	       {'time': 9,
		'status': 'complete',
		'url': '',
		'txt': 'complete'}, 
	    ],
	'c': [  
	       {'time': 2,
		'status': 'idle',
		'url': '',
		'txt': 'idle'},
	       {'time': 4,
		'status': 'running',
		'url': '',
		'txt': 'running'},
	       {'time': 8,
		'status': 'ifdh::cp foo bar',
		'url': '',
		'txt': 'ifdh::cp foo bar'},
	       {'time': 10,
		'status': 'complete',
		'url': '',
		'txt': 'complete'}, 
	    ],
      }
    )
    print tg.draw_boxes()

    class fakerow:
        def __init__(self, **kwargs):
             self.__dict__.update(kwargs)

    print '<hr>'
    testrows = [ 
         fakerow( jobid= 'job1', created = 1, status = "idle"),
         fakerow( jobid= 'job1', created = 6, status = "ifdh::cp whatever"),
         fakerow( jobid= 'job1', created = 3, status = "running"),
         fakerow( jobid= 'job1', created = 9, status = "completed"),
         fakerow( jobid= 'job2', created = 2, status = "idle"),
         fakerow( jobid= 'job2', created = 4, status = "running"),
         fakerow( jobid= 'job2', created = 6, status = "ifdh::cp whatever"),
         fakerow( jobid= 'job2', created = 10, status = "completed"),
         fakerow( jobid= 'job3', created = 2, status = "idle"),
         fakerow( jobid= 'job3', created = 4, status = "running"),
         fakerow( jobid= 'job3', created = 7, status = "ifdh::cp whatever"),
         fakerow( jobid= 'job3', created = 10, status = "completed"),
         fakerow( jobid= 'job4', created = 3, status = "idle"),
         fakerow( jobid= 'job4', created = 5, status = "running"),
         fakerow( jobid= 'job4', created = 9, status = "ifdh::cp whatever"),
         fakerow( jobid= 'job4', created = 11, status = "completed"),
        ]
    print tg.render_query(0,12,testrows,'jobid')
