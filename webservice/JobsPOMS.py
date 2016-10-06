#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar. 
List of methods: active_jobs, output_pending_jobs, wrapup_tasks    
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

#############
###TaskStaff	






	def wrapup_tasks(self, dbhandle, samhandle): # this function call another function that is not in this module, it use a poms_service object passed as an argument at the init. 
		now =  datetime.now(utc)
		res = ["wrapping up:"]
		for task in dbhandle.query(Task).options(subqueryload(Task.jobs)).filter(Task.status != "Completed", Task.status != "Located").all():
		     total = 0
		     running = 0
		     for j in task.jobs:
			 total = total + 1
			 if j.status != "Completed" and j.status != "Located":
			     running = running + 1

		     res.append("Task %d total %d running %d " % (task.task_id, total, running))

		     if (total > 0 and running == 0) or (total == 0 and  now - task.created > timedelta(days= 2)):
			 task.status = "Completed"
			 task.updated = datetime.now(utc)
			 dbhandle.add(task)

		# mark them all completed, so we can look them over..
		dbhandle.commit()

		lookup_task_list = []
		lookup_dims_list = []
		n_completed = 0
		n_stale = 0
		n_project = 0
		n_located = 0
		for task in dbhandle.query(Task).options(subqueryload(Task.jobs)).options(subqueryload(Task.campaign_obj,Campaign.campaign_definition_obj)).filter(Task.status == "Completed").all():
		    n_completed = n_completed + 1
		    # if it's been 2 days, just declare it located; its as 
		    # located as its going to get...
		    if (now - task.updated > timedelta(days=2)):
			 n_located = n_located + 1
			 n_stale = n_stale + 1
			 task.status = "Located"
			 task.updated = datetime.now(utc)
			 dbhandle.add(task)
			 if not self.poms_service.launch_recovery_if_needed(task.task_id):
			     self.poms_service.launch_dependents_if_needed(task.task_id)
		    elif task.project:
			 # task had a sam project, add to the list to look
			 # up in sam
			 n_project = n_project + 1
			 lookup_task_list.append(task)
			 basedims = "snapshot_for_project_name %s " % task.project
			 allkiddims = basedims
			 for pat in str(task.campaign_obj.campaign_definition_obj.output_file_patterns).split(','):
			     if pat == 'None':
				pat = '%'
			     allkiddims = "%s and isparentof: ( file_name '%s' and version '%s' with availability physical ) " % (allkiddims, pat, task.campaign_obj.software_version)
			 lookup_dims_list.append(allkiddims)
		    else:
			 # we don't have a project, guess off of located jobs
	 		 locflag = True
			 for j in task.jobs:
			     if j.status != "Located":
				 locaflag = False
			 if locflag:
			     n_located = n_located + 1
			     task.status = "Located"
			     task.updated = datetime.now(utc)
			     cherrypy.request.db.add(task)
			     if not self.poms_service.launch_recovery_if_needed(task.task_id):
				 self.poms_services.launch_dependents_if_needed(task.task_id)

		dbhandle.commit()

		summary_list = samhandle.fetch_info_list(lookup_task_list)
		count_list = samhandle.samweb_lite.count_files_list(task.campaign_obj.experiment,lookup_dims_list)
		thresholds = []
		for i in range(len(summary_list)):
		    # XXX
		    # this is using a 90% threshold, this ought to be
		    # a tunable in the campaign_definition.  Basically we consider it
		    # located if 90% of the files it consumed have suitable kids...
		    # cfrac = lookup_task_list[i].campaign_obj.campaign_definition_obj.cfrac
		    cfrac = 0.9
		    threshold = (summary_list[i].get('tot_consumed',0) * cfrac)
		    thresholds.append(threshold)
		    if float(count_list[i]) >= threshold:
			n_located = n_located + 1
			task = lookup_task_list[i]
			task.status = "Located"
			task.updated = datetime.now(utc)
			dbhandle.add(task)

			if not self.poms_service.launch_recovery_if_needed(task.task_id):
			    self.poms_servicelaunch_dependents_if_needed(task.task_id)

		res.append("Counts: completed: %d stale: %d project %d: located %d" %
			(n_completed, n_stale , n_project, n_located))

		res.append("count_list: %s" % count_list)
		res.append("thresholds: %s" % thresholds)
		res.append("lookup_dims_list: %s" % lookup_dims_list)

		dbhandle.commit()

		return "\n".join(res)
