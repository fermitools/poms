class time_grid:

     def render_query(self, tmin, tmax, rows, group_key):
         dlmap = self.group_time_data( rows, group_key)
         print "got dlmap:", dlmap
         self.add_time_data(tmin, tmax, dlmap)
         print "self.pmap is: ", self.pmap
         return self.draw_boxes()

     def group_time_data( self, rows, group_key ):
          result = {}
          lastkey = None
          for row in rows:
              key = getattr(row, group_key)
              if key != lastkey:
                  result[key] = []
                  lastkey = key
              result[key].append( {'time':row.created, 
                                   'status': row.status, 
                                   'txt':  row.status,
                                   'url':  getattr(row, 'url','') 
                                  })
          return result

     def status_color(self,str):
          if str.find("running") >= 0:
              return "#11ff11"
          if str.find("ifdh::cp") >= 0:
              return "#ddffdd"
          if str.find("idle") >= 0:
              return "#888888"
          return "#ffffff"

     def pwidth(self, t0, t1):
          return (t1 - t0) * 99 / (self.tdelta)

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

     def draw_boxes(self):
         rlist = []
         for id,plist in self.pmap.items():
             rlist.append("""
               <div class='row' style='margin:5px 5px;'>
                 <div class='label' style='text-align: right; width: 9%%; float:left'>%s</div>
                   <div clas='rowdata'  style='width: 90%%; float:left; clear: right;'>
                """ % id)
             for p in plist:
                 rlist.append("""
                       <a href='%s' title='%s'>
                         <div class='tbox' style='width: %d%%; background-color: %s; float:left;'>
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
