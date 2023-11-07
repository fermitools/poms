---
layout: page
title: Technical Design/campaign Slicing
---
* TOC
{:toc}
### Introduction

To actually launch jobs, campaigns need to be split into tasks. Trivially, one could simply split
the overall dataset into N chunks, and launch tasks for each of these chunks. However experimenters
want to track progress of large datasets by aspects of the metadata of the input files; i.e. what
detector, trigger, etc. the data is from. So we want to have a set of components by which we
want to slice the dataset into components.

### Defining components

The components will be defined in a given campaign which will be used in slicing the overall
dataset into per-task datasets. Let us take as an example an experiment with a near and far
detector, and triggers a, b, c, and d, and we are doing "keepup" processing -- we want to
run the files which appeared in the last day. Then each day we would take the overall dataset, and
generate 8 subset datasets, and launch tasks for them:

``-----------------------------``  
detector trigger date  
near a last day  
near b last day  
near c last day  
near d "  
far a "  
far b "  
far c "  
far d "  
``-----------------------------``

This should be data driven -- that is we ask the data handling metadata catalog what
values exist for each of these metadata tags, and then generate the table, above.

Towards that end, we will need to have in the Campaign, a mapping (as a json object?)
of tags names to dataset definition fragments, which will be used to subslice the
data in this Campaign for tracking. The tag names will be included in the project
names to allow selection/grouping by tags. There will then also be a "catchall" or
un-tagged dataset which is all the files in the parent dataset not included in the
tagged list -- this should properly be empty, but may not be, so we should always
build it and check it to see if it contains any files that need to be run that
the tagged sets missed.

We might also need a mapping in the Campaign of tags to command line options, so
that we can specify slightly different processing options for, say, near vs far
detector data, but all as part of one Campaign.

### Time slicing

One sort of time slicing is keepup processing -- we process the new files as they appear.

Another sort of slicing is to partition a relatively static dataset into chunks that spread it
evenly over some number of submissions, i.e. by using the ofset/modulus operators in SAM.

Secondarily, it is frequently wanted to do an initial 10% of the data which is spread approximately evenly
by time boundary, to watch for time-dependant data issues; and then run the remaining 90% in batches.
In this case, if we want our jobs to use d files per submission, we need to compute n = (total files / 0.9d)
and slice our dataset into n chunks; then first run a job batches which use groups of 10 ( 10% of each 1/Nth chunk),
to get our initial 10%, and then submit jobs for the 1/Nth chunks which exclude those processed in the first 10%.

### Combining slices

Finally, we need to slice by time and by type/tag to get datasets to submit. We need to take the time slices desired,
and the metadata slices of those we want to track separately, and also a catchall dataset which is the files in
the time slice not included in the metadata slices (which is hopefully empty, but if not, we need to run it)