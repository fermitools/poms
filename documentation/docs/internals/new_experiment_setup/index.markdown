---
layout: page
title: New Experiment Setup
---
* TOC
{:toc}
Lots of things:

Things we should't have to do over:

* Updated the fetch-users-from-voms script to get the experiment list
* Updating the metadata code in fife_wrap so we have wierdness in sam_metadata_dumper covered:
  * Mapped metadata values that look like sam ones but aren't to art.xxx parameters
  * Dealt with weird list of numbers as event numbers case
  * Checked that these were the same ones the FTS plugin that runs metadata_dumper for NOvA fixes.
  * (we should put in a ticket to the Art folks to fix this; it's annoying)

### POMS:

* Add experiment in GUI and/or database
* Add page for their errors codes: [https://cdcvs.fnal.gov/redmine/projects/offline_production_operations_service/wiki/_EXPERIMENT_error_codes_](https://cdcvs.fnal.gov/redmine/projects/offline_production_operations_service/wiki/_EXPERIMENT_error_codes_).  
* You can start with a link to [https://cdcvs.fnal.gov/redmine/projects/art/wiki/ArtExitCodes](https://cdcvs.fnal.gov/redmine/projects/art/wiki/ArtExitCodes).
    run the fetch users script to create the users
* Meet with experiment to pick an initial workflow (usually MonteCarlo)
* Pick a similar one from existing experiment, dump with Tagged Campaigns/Workflow ini file
* Edit config for new experiment, alternate options, etc.
* Upload to POMS with upload_wf
* Test with experimenters

### SAM instance:

* Register POMS, Exp. production proxy under suitable user in experiment
* file_types etc defined so we can declare files (for sbnd,icarus, cloned uboone's list)
* Added art.file_format_version etc. as place to put info from sam_metadata_dumper which
    is in wrong format.

### migrate_meta script:

    for vt in data_tiers file_formats file_types
    do
        samweb -e uboone list-values $vt |
        while read val
        do
        for e in sbnd icarus
        do
            if [ $vt = data_tiers ]
            then
                samweb -e $e add-value $vt $val "$val data" 
            else
                samweb -e $e add-value $vt $val
            fi
        done
        done
    done

    samweb -e icarus add-parameter dataset.tag string
    samweb -e icarus add-parameter params.process_name string
    samweb -e icarus add-parameter params.file_format_era string
    samweb -e icarus add-parameter params.file_format_version int
