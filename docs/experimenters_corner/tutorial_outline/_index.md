## Front page

* Service status
* Hold/allow jobs


## First Time Setup

#### Before we start
* make sure you can
  * log in somewhere
  * kdestroy (so you arent' relying on your krb5 creds)
  * setup things
  * submit a small job of this type
  * with above specifying on command line
* * software version,
* * SAM dataset
* * number of jobs
* Have a log/detailed notes of same.


#### Config Overview

(yes it needs a setup wizard...)

Three components, in left menu, to setup:

* Launch Template
  * "To prepare to launch jobs I ssh into xxx and source yyy and zzz"
* Job Type
  * "To submit a monte carlo job, I run exp_launch.py --dataset fred"
* Campaign Layer
  * "This is Fred's monte carlo run with version x and configs y"


#### Launch template

"To prepare to launch jobs I ssh into xxx and source yyy and zzz"

* left menu
* Compose Launch Template
* pick experiment (if not there, file ticket)
* Click [ + Add ]
* Give it a name (i.e. exppro_on_expgpvm02), a hostname (i.e. expgpvm02) , and account (i.e. exppro)
* Fill in a setup entry like "source /some/exp/setup; setup exp_sw %(version)s"
* NOTE: use %(version)s if you're setting up some version, so POMS can fill it in with the correct version from a given campagin layer later.
* hit Submit
* Bonus points: ssh into that account, add

      poms/cd/pomsgpvm01.fnal.gov@FNAL.GOV

* to .k5login


#### Job Type
"To submit a monte carlo job, I run 'exp_launch.py --mc --dataset fred'"
* left menu
* Compose Job Type
* pick experiment (if not there, file ticket)
* Click [ + Add ]
* Give it a name (i.e. exp_mc_type1)
* list Output File Patterns (i.e. mc_out%.root) of outputs that will get declared to SAM
* Launch script
  * give it a launch script to run, that will have parameters passed to it.
  * if your usual launch script doesn't accept command line parameters for, say, dataset definition, etc. you need to put in a shell function that does, then call that...
  * I recommend fife_launch :-)
* Definition Parameters
  * click [/] (w/ pencil) to edit
  * this is a mechanism to define things that might get overridden further, so it's slightly tricky to do right the first time; We'll talk more about this later.
  * left hand "Key" column must be unique
  * use [+] button to add more pairs.
  * name value pairs that make parameters to script
  * If its like -O sect.var=value split it as [-O sect.var=] [value]
  * if option needs space after it, include in box [-g ] [group]
* Recovery Launches
  * click [/] (w/ pencil) to edit
  * Pick recovery type for up to 5 recoveries.
  * recommend 1. consumed_status 2. pending files
  * hit [Accept]
* hit [Submit]


#### Campaign Layer
"This is Fred's monte carlo run with version x and configs y"
* left menu
* Compose Campaign Layer
* pick experiment (if not there, file ticket)
* Click [ + Add ]
* Give it a name i.e. "Spring16CosmicsMC"
* List VO Role (i.e Production or Calibration...)
* Software version -- exp software version that will be in output metadata
* Dataset -- full input dataset (i.e. mc config files, raw files to process, etc.)
* Dataset split type
  * If Dataset is large; and not defined as "draining"..
  * usually mod(k) to split into k pieces is good
  * new(window=2d,start_time=123456) for two days of data per submission
  * let us know if other split types are useful (i.e. by subrun?)
* Completion type/percent -- (i.e. Located @ 95% (if using SAM))
* Parameter Overrides
  * edit with [/] (w/pencil) click, as with Job Types, above
  * here is where picking good parameters in the Job Type pays off
  * If something needs to be different/added to the launch in the Job Type for this campaign, you can add it here
  * anything with matching Key here overrides one from Job Type
  * new Keys are added to options
  * hit [Accept] when done
* Depends On
  * Will do more later, but:
  * edit with [/] (w/pencil) click
  * Can pick other exisitng Campaign layer for your experiment (but not yet, as we don't have any)
  * Can pick this campaign layer, means launch another batch as each one completes.
  * Specify particular output file pattern from that campaign's output you use as input.
  * hit [Accept] when done
* Launch Template
  * at the moment we have 1 to choose from, can pick any you have, fields show up.
* Job Type
  * at the moment we have 1 to choose from, can pick any you have, fields show up.
* Hit [Submit]


#### Actually Launching
* Launch by hand
  * left menu
  * Active for exp
  * Click on campaign layer name
  * Launch Campaign Jobs Now
* Launch via cron (show uBooNE Electron Lifetime for example)
  * left menu
  * Active for exp
  * Click on campaign layer name
  * Clikc on Schedule Future Job Launches
  * Fill in entry form for crontab


#### Watching what goes on
* left menu
* Active for exp
* Click on Campaign Layer name
* Several options
  * By submission-jobs: (control?)-click Submission Time Bars
* * Page shows time bars for each submission
* * Can click through to jobs from that submission
* * Can click through again to job triage
  * By submission-files: Campaign Submission Files
* * Can click through to specific file listings


#### Debugging launch problems
* Make a debug Job Type
  * clone existing one
  * name it Debug-xxx
  * stick an "printenv ; echo" in front of command you usually run
  * switch your campaign to use the Debug-xxx Job Type
* launch may be working due to kerberos ticket interactively
  * log into account
  * kdestroy
  * test setup/launch using exp. proxy
