#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar. 
List of methods: active_jobs, output_pending_jobs, update_jobs    
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016. 
'''

from model.poms_model import Experiment, Job, Task, Campaign, Tag, JobFile
from datetime import datetime
#from LaunchPOMS import launch_recovery_if_needed
#from poms_service import poms_service



class JobsPOMS:
	
	def __init__(self, ps):
		self.poms_service=ps

###########
###JOBS
	def active_jobs(self, dbhandle):
		 res = [ "[" ]
		 sep=""
		 for job in dbhandle.query(Job).filter(Job.status != "Completed", Job.status != "Located", Job.status != "Removed").all():
		      if job.jobsub_job_id == "unknown":
			   continue
		      res.append( '%s "%s"' % (sep, job.jobsub_job_id))
		      sep = ","
		 res.append( "]" )
		 res = "".join(res)
		 gc.collect(2)
		 return res

	def output_pending_jobs(self,dbhandle):
		 res = {}
		 sep=""
		 preve = None
		 prevj = None
		 for e, jobsub_job_id, fname  in dbhandle.query(Campaign.experiment,Job.jobsub_job_id,JobFile.file_name).join(Task).filter(Task.campaign_id == Campaign.campaign_id, Job.jobsub_job_id != "unknown", Job.task_id == Task.task_id, Job.job_id == JobFile.job_id, Job.status == "Completed", JobFile.declared == None, JobFile.file_type == 'output').order_by(Campaign.experiment,Job.jobsub_job_id).all():
		      if preve != e:
			  preve = e
			  res[e] = {}
		      if prevj != jobsub_job_id:
			  prevj = jobsub_job_id
			  res[e][jobsub_job_id] = []
		      res[e][jobsub_job_id].append(fname)
		 sres =  json.dumps(res)
		 res = None   #Why do you need this? 
		 return sres


	def update_job(self, dbhandle, loghandle, rpstatus, task_id = None, jobsub_job_id = 'unknown',  **kwargs):
		 
		 if task_id:
		     task_id = int(task_id)

		 host_site = "%s_on_%s" % (jobsub_job_id, kwargs.get('slot','unknown'))

		 jl = dbhandle.query(Job).options(subqueryload(Job.task_obj)).filter(Job.jobsub_job_id==jobsub_job_id).order_by(Job.job_id).all()
		 first = True
		 j = None
		 for ji in jl:
		     if first:
			j = ji
			first = False
		     else:
			#
			# we somehow got multiple jobs with the sam jobsub_job_id
			#
			# mark the others as dups
			ji.jobsub_job_id="dup_"+ji.jobsub_job_id
			dbhandle.add(ji)
			# steal any job_files
			files =  [x.file_name for x in j.job_files ]
			for jf in ji.job_files:
			    if jf.file_name not in files:
				njf = JobFile(file_name = jf.file_name, file_type = jf.file_type, created =  jf.created, job_obj = j)
				dbhandle.add(njf)

			dbhandle.delete(ji)
			dbhandle.flush()      #######################should we change this for dbhandle.commit()

		 if not j and task_id:
		     t = dbhandle.query(Task).filter(Task.task_id==task_id).first()
		     if t == None:
			 loghandle("update_job -- no such task yet")
			 rpstatus="404 Task Not Found"
			 return "No such task"
		     loghandle("update_job: creating new job") 
		     j = Job()
		     j.jobsub_job_id = jobsub_job_id.rstrip("\n")
		     j.created = datetime.now(utc)
		     j.updated = datetime.now(utc)
		     j.task_id = task_id
		     j.task_obj = t
		     j.output_files_declared = False
		     j.cpu_type = ''
		     j.node_name = ''
		     j.host_site = ''
		     j.status = 'Idle'

		 if j:
		     loghandle("update_job: updating job %d" % (j.job_id if j.job_id else -1))

		     for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code']:    ######?????????????? does those fields come in **kwargs or are juste grep from the database direclty

			 if field == 'status' and j.status == "Located":
			     # stick at Located, don't roll back to Completed,etc.
			     continue

			 if kwargs.get(field, None):
			    setattr(j,field,kwargs[field].rstrip("\n"))
			 if not getattr(j,field, None):
			    if field != 'user_exe_exit_code':
				setattr(j,field,'unknown')

		     if kwargs.get('output_files_declared', None) == "True":
			 if j.status == "Completed" :
			     j.output_files_declared = True
			     j.status = "Located"

		     for field in ['project','recovery_tasks_parent' ]:
			 if kwargs.get("task_%s" % field, None) and kwargs.get("task_%s" % field) != "None" and j.task_obj:
			    setattr(j.task_obj,field,kwargs["task_%s"%field].rstrip("\n"))
			    loghandle("setting task %d %s to %s" % (j.task_obj.task_id, field, getattr(j.task_obj, field, kwargs["task_%s"%field])))


		     for field in [ 'cpu_time', 'wall_time']:
			 if kwargs.get(field, None) and kwargs[field] != "None":
			    setattr(j,field,float(kwargs[field].rstrip("\n")))

		     if kwargs.get('output_file_names', None):
			 loghandle("saw output_file_names: %s" % kwargs['output_file_names'])
			 if j.job_files:
			     files =  [x.file_name for x in j.job_files if x.file_type == 'output']
			 else:
			     files = []

			 newfiles = kwargs['output_file_names'].split(' ')
			 for f in newfiles:
			     if not f in files:
				 if len(f) < 2 or f[0] == '-':  # ignore '0', '-D', etc...
				     continue
				 if f.find("log") >= 0:
				     ftype = "log"
				 else:
				     ftype = "output"

				 jf = JobFile(file_name = f, file_type = ftype, created =  datetime.now(utc), job_obj = j)
				 j.job_files.append(jf)
				 dbhandle.add(jf)

		     if kwargs.get('input_file_names', None):
			 loghandle("saw input_file_names: %s" % kwargs['input_file_names'])
			 if j.job_files:
			     files =  [x.file_name for x in j.job_files if x.file_type == 'input']
			 else:
			     files = []

			 newfiles = kwargs['input_file_names'].split(' ')
			 for f in newfiles:

				if len(f) < 2 or f[0] == '-':  # ignore '0', '-D', etc...
			 		continue

			 	if not f in files:
					 jf = JobFile(file_name = f, file_type = "input", created =  datetime.now(utc), job_obj = j)
					 dbhandle.add(jf)


		     if j.cpu_type == None:
		     	j.cpu_type = ''

		     loghandle("update_job: db add/commit job status %s " %  j.status)

		     j.updated =  datetime.now(utc)

		     if j.task_obj:
		     	newstatus = self.compute_status(j.task_obj)
			if newstatus != j.task_obj.status:
			     j.task_obj.status = newstatus
			     j.task_obj.updated = datetime.now(utc)
			     j.task_obj.campaign_obj.active = True

		     dbhandle.add(j)
		     dbhandle.commit()

		     loghandle("update_job: done job_id %d" %  (j.job_id if j.job_id else -1))

		 return "Ok."



    @cherrypy.expose
    def show_task_jobs(self, task_id, tmax = None, tmin = None, tdays = 1 ):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'show_task_jobs?task_id=%s' % task_id)

        jl = dbhandle.query(JobHistory,Job).filter(Job.job_id == JobHistory.job_id, Job.task_id==task_id ).order_by(JobHistory.job_id,JobHistory.created).all()
        tg = time_grid.time_grid()
        
	class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
        items = []
        extramap = {}
        laststatus = None
        lastjjid = None
        for jh, j in jl:
            if j.jobsub_job_id:
                jjid= j.jobsub_job_id.replace('fifebatch','').replace('.fnal.gov','')
            else:
                jjid= 'j' + str(jh.job_id)

            if j.status != "Completed" and j.status != "Located":
                extramap[jjid] = '<a href="%s/kill_jobs?job_id=%d"><i class="ui trash icon"></i></a>' % (self.path, jh.job_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'
            if jh.status != laststatus or jjid != lastjjid:
                items.append(fakerow(job_id = jh.job_id,
                                  created = jh.created.replace(tzinfo=utc),
                                  status = jh.status,
                                  jobsub_job_id = jjid))
            laststatus = jh.status
            lastjjid = jjid

        job_counts = self.format_job_counts(task_id = task_id,tmin=tmins,tmax=tmaxs,tdays=tdays, range_string = time_range_string )
        key = tg.key(fancy=1)

        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)
        #screendata = screendata +  tg.render_query(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)

        if len(jl) > 0:
            campaign_id = jl[0][1].task_obj.campaign_id
            cname = jl[0][1].task_obj.campaign_obj.name
        else:
            campaign_id = 'unknown'
            cname = 'unknown'

        task_jobsub_id = self.task_min_job(task_id)

        template = self.jinja_env.get_template('show_task_jobs.html')
        return template.render( blob=blob, job_counts = job_counts,  taskid = task_id, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), extramap = extramap, do_refresh = 1, key = key, pomspath=self.path,help_page="ShowTaskJobsHelp", task_jobsub_id = task_jobsub_id, campaign_id = campaign_id,cname = cname, version=self.version)



