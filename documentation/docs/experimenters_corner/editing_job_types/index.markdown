---
layout: page
title: Editing Job Types
---
* TOC
{:toc}
This lets you set the commands to launch a job of a given type.

First you pick the experiment appropriately,then you choose an campaign from the list,
or click "[+]Add"

Then you get to pick the name of this definition, the launch script, and parameters to that
script that this job type uses.

###  Name

A name for this job type

#### Input Files Per Job

If you have a fixed number of input files per job, you can enter it here.

#### Output Files Per Job

If you have a fixed number of output files per job, you can enter it here.

#### Output File Patterns

A database-'like' , percent sign wildcard, file pattern that matches your output files. Being more specific than the default '%' lets us distinguish actual output files from log files, etc.

#### Launch Script

The commands you will run to launch a job of this type. This will have [Variable Substitution]({{ site.url }}/docs/internals/variable_substitution)  performed.