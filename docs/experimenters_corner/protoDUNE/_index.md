In order to submit and monitor your campaigns through POMS, you will need to do three stages create compose a launch template, a job type, and a campaign stage. This example is specific to ProtoDUNE collaboration.

#### Compose Launch template

For compose a launch template you need to click on Compose Launch template (look image below). This template can be reused and re-edit later. You can re-use, clone, and modify for different campaigns. So you can use the same template for different processing campaigns. As a user, you choose your experiment and push the bottom "add" in order to create a new template. Those are also useful for other members of the experiment.

![POMSMenuComposeLaunch.png](/docs/images/POMSMenuComposeLaunch.png)

For this example, we would like to copy a previous template clicking on the icon shown by the red arrow.

![LaunchTemplatewitharrow.png](/docs/images/LaunchTemplatewitharrow.png)

Then you will have the chance to edit the template.

![LaunchTemplateEdit.png](/docs/images/LaunchTemplateEdit.png)

**Name:** Define the name of your template (MonteCarlo)  
**Host:** Define the interactive node or machine you are going to use to launch your jobs. e.g. dunegpvm01  
**Account:** Define the user account for the launch e.g. dunepro  
**Setup:** You define the environment variable, setups, or scripts that you usually setup on your own machine

#### Compose Job Type (campaign definition):
![ComposeJobType.png](/docs/images/ComposeJobType.png)

After you fill the job type format, you can re-use, clone, and modify for different campaigns. Those are also useful for other members of the experiment.

![JobTypes.png](/docs/images/JobTypes.png)

You can copy the Job Type (template) in the same way it was done for the previous stage (clicking the icon)

![CampaignDefinitionEditor.png](/docs/images/CampaignDefinitionEditor.png)

**Name:** A name that describes the kind of campaign you are running.  
**Output file patterns:** The output pattern you are interested in your campaign (eg. %.root)  
**Launch Script:** In this field, you need to put the script that you run to submit jobs in your machine.  
**Definition Parameters:** The arguments your script (included in Launch script) use for the submission.  
**Recovery Launches:** This field defines your recovery launches in case needed.

#### Compose the campaign layer:
![ComposeCampaignStage.png](/docs/images/ComposeCampaignStage.png)

This layer defines your specific campaign (related with a specific production task), in this step you use the launch template and the job type define in the previous steps (or you can use an existing one). Then you can copy one of the campaigns forms and edit it for your own campaign.

![campaignStages.png](/docs/images/campaignStages.png)

You can edit the fields, there you will pick a launch template and a Job Type.

![CampaignStageEditor.png](/docs/images/CampaignStageEditor.png)

**Name of the campaign:** A name that describes your campaign properly
**VO role:** The role in the VO (eg. analysis, production)  
**State:** Active or Inactive. Mark your campaigns as inactive if you do not want to see them in your main menu.  
**Software Version:** In your experiments, you can have different versions of your code, this field help you to track the one that you use for that specific campaign.  
**DataSet:** The SAM definition (dataset) that you want to process, you can also define that in your script then you leave this field as none  
**DataSet split type:** POMS can split the dataset in order to do sub-dataset and make the tracking task easier. It is also useful to prevent problems handling huge datasets inside SAM projects.  
**Completion type:** Then the user have two options: located and complete.  
**Located:** This option suggests that your completed threshold (for launch dependence campaigns) depends on the number of job with all their files located. A located file reference a file which has children declare on SAM.  
**Complete:** This option suggests that your completed threshold (for launch dependence campaigns) depends on the number of jobs that ended with error code 0 but it does not check if the output files are located. A located file reference a file which has the children declare on SAM.

**Completion pct:** it is the percentage of completion of jobs that you define to determine that specific campaign is successful (complete). Fill with a number (0-100]  
**Parameters override:** This field is useful if you want to change the ones that are in the template  
**Depends on:** If your campaign depends on other campaigns. (eg. mergeana depends on reco campaign)  
**Launch template:** This field you just choose a template that you have already create in advance (description above)  
Job type This field you just choose a job type that you have already create in advance (description above)