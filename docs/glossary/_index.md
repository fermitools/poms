This page defines various terms used in POMS, and help links in the application may point here.

### Campaign

A Campaign is a collection of stages which define the steps to setup, start and run to achieve the goal of data processing.

### Campaign Stage

A high-level group of processing work. It based upon a user/group request to run multiple multi-job
submissions described by a [Job Type]({{< ref "#job-type" >}}) under a given [Role]({{< ref "#role" >}}) to process a given [Dataset]({{< ref "#dataset" >}}) with a particular software version.

### Experimenter

A person who requests a task, and/or an operator or other person in the system

### Experiment

An experiment virtual organization name associated with a request.

### Job
An individual batch-system job, part of a given Task's job submission, which associated with a Campaign.
POMS ignores jobs which are not part of a Campaign. Jobs are considered to be in one of 5 high level
states:

* Idle -- The job is waiting to be scheduled somewhere by the batch system
* Running -- The job has been started by the batch system, and is still running
* Completed -- the job has finished running
* Held -- the batch system has stopped the job for some reason (using too much memory, etc)
* Located -- the job was not only completed, but its output files have locations in the data handling system.

where the first 4 are the usual states are defined by Condor, and the fifth is peculiar to POMS. Within the Running state, there may be more details logged; i.e. "Running: running user executable" , "Running: copying in files", etc. where these can be determined from the ifdh joblogs.

### Service

A system that we depend on to run jobs, whose status is in some way determinable.

### Job Type

(Task Definition/Campaign (layer) Definition)  

All the info you need to run a particular category of job -- i.e. "how do I launch a NOvA montecarlo job"
or "how do I launch a Minerva calibration keepup job". This is filled in with information from a given
Campaign to know how to run a submit a given Task.

### Submission

(Task)  

A specific multi-job launch. It has a specific command line used to launch it, and it may have
output files for whose appearance we await in the data handling system before we declare the task
complete. It has states defined in terms of the jobs it generates as follows:

* Idle -- all of its jobs are Idle
* Running -- one or more of its jobs are Running
* Completed -- all of its jobs are Completed or Declared
* Located -- all of its jobs are Located

If another Submission in (another Campaign Layer) needs the output of this one to be launched, it must wait for this task to be in state "Located"
before launching; or if the Submissions are generated with a Draining dataset definition, one waits for the previous
task for this Campaign to complete before launching another.