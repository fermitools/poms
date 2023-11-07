---
layout: page
title: Proposals/campaign Stage Types Proposal
---
* TOC
{:toc}
In recent POMS releases we've added a field for stage types, but it is so far largely unused.

In upcoming releases I would like to use this field, and back it up with some code. Possible
campaign stage types are:

* Test -- means this is a stage designed to test a Job Type and may not be part of a campaign, etc.
* Data Transfer -- means this stage is used to move data around, and it's output dataset is the same as its input dataset
* Data Merge -- means that multiple dependencies entering this stage all wait for all of them and do one submission
* Resplit -- means that any split types on this stage should be applied to the input dataset; and also it acts as a data merge stage
* Generator -- means that
  * there is no input dataset and that
  * the submissions will generate poms_depends_${POMS_TASK_ID}_${i} datasets for i in 1..${POMS_NUM_DEPENDENCIES}
  * Any split_type info is really a parameter list rather than a dataset list, so it should set %(parameter)s (as well as %dataset)s for backwards compatability)

These should be reflected in the editor with suitable