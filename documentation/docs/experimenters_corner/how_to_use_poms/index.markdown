---
layout: page
title: How To Use Poms
---
* TOC
{:toc}
In order to be able to use POMS you need to be affiliated with a Fermilab experiment and have a computer account at Fermilab.

## How to access to the POMS services

Access to the following URL https://pomsgpvm01.fnal.gov/poms/ and use your Fermilab services credential.

### You need to do some configuration before you launch jobs. This a three steps process, following the links under Configure work in the left menu of POMS webpage.

#### 1) Compose Launch template

This template can be reused and re-edit later. You can re-use, clone, and modify for different campaigns. So you can use the same template for different processing campaigns. As a user, you choose your experiment and push the bottom "add" in order to create a new template. Those are also useful for other members of the experiment. You need to fill:

* Name: Define the name of your template (MonteCarlo)
* Host: Define the interactive node or machine you are going to use to launch your jobs (novagpvm01)
* Account: Define the user account for the launch (eg. {USER}, novapro, minervapro, minervacal, uboonepro, etc )
* setup: You define the environment variable, setups, or scripts that you usually setup on your own machine (e.g. setup_nova, launch script)

The **Account** in question will need the following .k5login entries:

    poms/cd/pomsgpvm01.fnal.gov@FNAL.GOV


#### 2) Compose Job Type:

After you fill the job type format, you can re-use, clone, and modify for different campaigns. Those are also useful for other members of the experiment.

* Name: A name that describes the kind of campaign you are running (eg. Nova raw2root keepup FD, rock_neutrinoMC, minerva_cal)
* input files per job: The expected number of input files per job, fill the field as 0 if you are not sure do not leave it empty (eg. 1 if you just receive the .raw data as an input file)
* output files per job: The expected number of output files per job, fill the field as 0 if you are not sure do not leave it empty (eg. 3 if you expected to produce one .root .log .json file per every input file )
* output file patterns: The output pattern you are interested in your campaign (eg. %.root)
* Launch Script: In this field, you need to put the script that you run to submit jobs in your machine.
* Definition Parameters: The arguments your script (included in Launch script) use for the submission.
* Recovery Launches: This field defines your recovery launches in case needed. Your options are: added_files, consumed_status, pending files, proj_status


#### 3) Compose the campaign stage:

This stage define your specific campaign (related with a specific production task), in this step you use the launch template and the job type defined in the previous steps (or you can use an existing one)

* Name of the campaign: A name that describes your campaign properly (eg. NOvA Raw2Root Keepup fardet S16-11-02)
* VO role: The role in the VO (eg. analysis, production)
* State (active or inactive)
* Software Version: In your experiments, you can have different versions of your code, this field help you to track the one that you use for that specific campaign.
* DataSet: The SAM definition (dataset) that you want to process, you can also define that in your script then you leave this field as none
* DataSet split type: POMS can split the dataset in order to do sub-dataset and make the tracking task easier. It is also useful to prevent problems handling huge datasets inside SAM projects.
* Completion type: Then the user have two options: located and complete.

> _Located: This option suggest that your completed threshold (for launch dependence campaigns) depends on the number of job with all their files located. A located file reference a file which has children declare on SAM._
> _Complete: This option suggest that your completed threshold (for launch dependence campaigns) depends on the number of jobs that ended with error code 0 but it does not check if the output files are located. A located file reference a file which has the children declare on SAM._

* Completion pct: it is the percentage of completion of jobs that you define to determine that specific campaign is successful (complete).
* Parameters override: This field is useful if you want to change the ones that are in the template
* Depends on: If your campaign depends on other campaigns. (eg. calibration can depend on RawtoRoot)
* Launch template: This field you just choose a template that you have already create in advance (description above)
* Job type This field you just choose a job type that you have already create in advance (description above)

__IMPORTANT__: It is important to keep in mind that until now the user just configures the campaign to launch but the submission has not happend. In order to submit jobs follow the step below.

### Launch the job submission

#### How to launch the jobs:

* After Compose the campaign stage, in order to launch jobs, the user should go the campaign stages under Campaign Data (left menu) and click.
* Then you will see all the active campaign for the experiment(s) you belong.
* Clicking on the campaign name move you to the campaign info page where the user can monitor the status of the jobs that belong to that specific campaign.
* Clicking the bottom Launch Campaign Jobs Now the user can submit jobs for the specific campaign configuration.

Furthermore, the user can use the option Schedule Future Job Launches which provide a kind of "crontab" for schedule your submissions. The scheduled submissions are sent by POMS and there is not crontab in your interactive node (experiment machine).

### Files
* <a href="/docs/files/POMS_ICARUS_SBND_2018_02_14.pdf" download>POMS_ICARUS_SBND_2018_02_14.pdf</a>
