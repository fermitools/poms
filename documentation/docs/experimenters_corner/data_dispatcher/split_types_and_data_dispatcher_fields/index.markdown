---
layout: page
title: Split Types and Data Dispatcher Fields
---
* TOC
{:toc}

> Some of the split types in POMS are essentially [deprecated](#simple) due to the ability to utilize complex MQL queries to specify files within a project. However, split types will continue to work with Data Dispatcher the same as they do with SAM.
* To utilize split-types for your campaign stages, you can set them up the same as you would with a SAM campaign. This can be done via ini file, or the campaign editor.
* If using a split type, ensure that:
	* The "**data_dispatcher_project_id**" field ***IS*** blank.
	* The "**data_dispatcher_dataset_query**" ***IS NOT*** blank, and is in the format required by the selected split-type.

## Notes on usage of data dispatcher fields:

* Stages **require** either a `data_dispatcher_dataset_query` **OR** `data_dispatcher_project_id`.

### data_dispatcher_project_id

* An Integer representing an existing project id.

* Consider this field to be a project_id ***override***, meaning that POMS will disregard other configuration details such as split type, and `data_dispatcher_dataset_query`, and ***only*** submit the data dispatcher project defined in the `data_dispatcher_project_id` field.

* This field should not be used if you are utilizing a split_type for your campaign/campaign stage because projects cannot be altered after they are created. If using a split type, please provide a `data_dispatcher_dataset_query` instead.

* This field is especially helpful if experimenters want to restart, re-submit, or continue processing a specific project, rather than create a new one.

### Example of data_dispatcher_project_id override trait
        
* In the example below, POMS will use the project with id=42 for each submission
        
> ```plaintext
> data_dispatcher_dataset_query = files from poms_sample:some_dataset
> data_dispatcher_project_id = 42
> ```
        
* In the next example, POMS will define a new project with the files returned from running 'files from poms_sample:some_dataset'

> ```plaintext
> data_dispatcher_dataset_query = files from poms_sample:some_dataset
> data_dispatcher_project_id = None
> ```
        
* *Note: To reduce confusion, the 'data_dispatcher_project_id' field may be renamed to "*data_dispatcher_project_id_override*" in the future.*

### data_dispatcher_dataset_query
    
* An **[mql query](https://metacat.readthedocs.io/en/latest/mql.html)** (string) that serves as the baseline for the files to be used within a data dispatcher project.

* Instead of merely entering a dataset name as was done with SAM, experimenters are now required to provide an MQL query to establish a baseline project. POMS may then reference and modify this baseline to comply with the requirements of a split type, or dependency launch.

* Utilizing an MQL query rather than a dataset presents numerous advantages for POMS users, including:
* ***Comprehensive***: All previous POMS data dispatcher submissions for a given campaign/stage are visible through a simple and intuitive UI. Project history can be filtered on the campaign/campaign_stage details pages. This UI encompasses the campaign, stage, and submission information, alongside the query utilized, project status, project files, and their statuses within a particular project submission.
    
    * Project Details are delineated on the left-hand side, and are filtered to the current campaign/campaign stage. Additional filters include project id, owner, and state.
        
    * Project files are situated on the right-hand side, and are displayed by selecting "View Handles" on the desired project.
        
        * Files can be sorted by name, namespace, and state.
        * The background color on each file's card header indicates its current state:
            * *Green: Done*
            * *Red: Failed*
            * *Orange: Reserved*
            * *Yellow: Initial*
        <center>
            <img name="Data_Dispatcher_UI_1" src="{{ site.url }}/docs/images/dd_ui_1.png">
            <label style="font-size: 12px; margin-bottom: 20px;" for="Data_Dispatcher_UI_1">
                <b>Figure 3</b>: Campaign Project Submission History :  This UI is available on the <i>data_dispatcher_overview</i>, <i>campaign_overview</i>, and <i>campaign_stage_info</i> pages.
                <br>
            </label>
        </center>
        
* ***Versatile***: Queries empower experimenters to create highly customized file sets for project submissions, offering the flexibility to alter them at any time and include files from multiple namespaces/datasets:
    
* <b><i><span id="simple">Simple</span></i></b>: Potentially reduces the need for certain split types:
    
    * In the SAM setup, some split types, such as the 'byrun' split type, filter input datasets based on metadata like 'run_number', while others might use the creation date or other metadata.
        
    * With Data Dispatcher, employing an MQL query as the baseline permits many of these additional configurations to be bypassed by simply appending filters to the baseline query, thereby streamlining the process.
        
        > **data_dispatcher_dataset_query** = files from poms_samples:some_dataset where core.run_number = 1202 and created_timestamp >= '2023-08-01' and created_timestamp <= '2023-08-31'

* ***Fast***: Facilitates rapid launch of recovery, dependency, or custom submissions in the event of an error with the initial query, or rather than waiting for POMS to submit a follow-up job:
    
    * Ex 1: Should some files be missed in a submission, and the project is already halfway complete, there's no need to create a new dataset or relaunch every file. Experimenters can effortlessly update the query to include specific files, launch, and monitor both projects in real-time:
        
        > **data_dispatcher_dataset_query** = fids 12345, 67891, ...

    * Ex 2: Suppose your dataset is missing some files from a parent dataset, and POMS encountered an issue launching a dependency submission. Your team urgently needs this task completed and is awaiting the resolution of the bug by the POMS developers. 
        
        * Instead of waiting, your team has the option to create a temporary dependency stage, or temporarily alter the `data_dispatcher_dataset_query` of an existing stage to obtain the necessary files promptly:
            
            > **data_dispatcher_dataset_query** = parents(files from poms_sample:some_dataset)
            > Or
            > **data_dispatcher_dataset_query** = parents(fids 12345, 678910,... )
            
    * For guidance on constructing MQL queries, refer to: [Metadata Query Language](https://metacat.readthedocs.io/en/latest/mql.html)