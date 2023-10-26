POMS supports several dataset split types, and more can be added as plugins. We will attempt to document them here as they are added. They are specified with a function-like notation:

  **name ( parameters )**

with a **name** and **parameters** as given. The following names are supported, each may have parameters as described in the following sections.


##### byrun

The byrun type takes parameters low and high giving a range of run numbers, as in byrun(low=3,high=12) which specifies splitting the dataset into runs starting at run number 3 and going up through run 12, inclusive.

##### draining

The draining split type doesn't really do anything, and takes no parameters. It is a way to indicate the dataset definition you have given is a "draining" dataset, and should exclude any files that have already been processed, so it should just be re-used on each submission.

##### list

The list split type takes a comma separated list of dataset names as a parameter, and will submit with each one in turn as the dataset. The input dataset given for the overall campaign is not used to compute the splits, but should be the union of the listed data. So using list with dataset set to ds1,ds2,ds3,.. will yeild dataset "ds1" on the first submission, dataset "ds2" on the second, etc.

##### mod

This takes one integer parameter, i.e. mod(5) which will split the dataset into 5 chunks, taking every fifth file with a differing offset for each submission.

##### new

This type is supposed to take new files since the last submission, but has lots of parameters to do variations of time-window based slices of files:

* window= Lets you specify a time window to group together; that is if you want to process files a week at a time you can use window=1w.
* round= Lets you specify a timeperiod to round things to (i.e. round to nearest day).
* fts= time to assume the FTS takes to process files.
* localtime= set to 1 to use localtime rather than GMT for time windows
* firsttime=n time to start

So if you want catch up and to do one-week chunks starting at unix time 1497934800: (i.e. from "date -D 2017-06-20 +%s")
new(firsttime=1497934800, window=1w)  
it will start by picking files in the week staring at that datae, then next time the next week's worth, etc.

##### nfiles

This will submit some number of files each submission using "offset" and "limit" i the dataset.

### New in v4_0_0

##### drainingn(k)

This is a new snapshot based splitter, which yeilds up to k files not yet processed each time. Files already processed are tracked with a snapshot stored in cs_last_split, which can be cleared to "reset" the splitter and start over.

##### stagedfiles(k)

This is a new snapshot based splitter, which works off of a project name, not a dataset name. You can give it the project name associated with a prestage script, and it will deliver up to k files marked "consumed" by that project each time it is called.
It also keeps a snapshot of the files delivered so far in cs_last_split, which can be cleared to "reset" the splitter and start over.