---
layout: page
title: POMS Campaign_setup
---
* TOC
{:toc}

Historically, campaigns in POMS have been configured to run exclusively on SAM. However, with some experiments transitioning to Data Dispatcher while others continue to utilize SAM, it's imperative for experimenters to specify the desired ***Data Handling Service*** for each campaign. By default, POMS is configured to operate with SAM, but this setting can be altered during the campaign creation or editing process as follows:
### By Campaign INI File 
* #### Section: [campaign]
	* Designate: *data_handling_service* = ***data_dispatcher***
    
		```ini
		[campaign]
	  experiment=hypot
	  poms_role=analysis
	  name=dd_sample_campaign_1
	  data_handling_service=data_dispatcher
	  state=Active
	  campaign_keywords={}
	  campaign_stage_list=dd_sample_campaign_stage_1
		```

* #### Section: [campaign_stage]
	* Designate **ONE** of the following: 
	* *data_dispatcher_dataset_query* = "some mql query" (as string)
	* *data_dispatcher_project_id* = your_project_id (as integer)
		```ini
		[campaign_stage dd_sample_campaign_stage_1]
		software_version={{version}}
		output_ancestor_depth=1
		cs_split_type=drainingn(5)
		data_dispatcher_dataset_query=files from {{namespace}}:{{dataset_name}}
		data_dispatcher_project_id={{data_dispatcher_project_id}}
		completion_pct=100.0
		merge_overrides=False
		stage_type=test
		default_clear_cronjob=False
		```

	> **Note**: The above example defined both *data_dispatcher_dataset_query* ***and*** *data_dispatcher_dataset_project_id*  for *informational purposes only*
	> *  Only one of these should be used for each campaign stage. See [usage notes](#notes-on-usage-of-data-dispatcher-fields) below for details.

### By GUI Campaign Editor
* #### Campaign
	* Double click on your campaign
		* Select ***data_dispatcher*** as the *data_handling_service* by selecting it from the dropdown.
		* Click Save

	<center>
		<img name="Campaign_Editor_1" src="{{ site.url }}/docs/images/Campaign_Editor_1.png">
		<label style="font-size: 12px;" for="Campaign_Editor_1">
			<br>
			<b>Figure 1</b>: Campaign Editor, note the dropdown menu next to <i>data_handling_service</i>
		</label>
	</center>

* * *

* #### Campaign Stage
	* Enter *data_dispatcher_dataset_query* **OR** *data_dispatcher_project_id*
	* Click Save
	> By selecting data_dispatcher as the *data_handling_service*, the remaining campaign setup will follow a flow similar to what users are accustomed to, with a few key differences:
	> * *sam_dataset_or_split_data* is grayed out and cannot be modified.
	> * A new field *data_dispatcher_dataset_query* is now used in place of the sam_dataset. **IMPORTANT**: this should be an MQL query, not a dataset.
	> * A new *data_dispatcher_project_id* exists and takes a numerical value, which is used as the project id for each submission for this stage.

	<center>
		<img name="Campaign_Editor_2" src="{{ site.url }}/docs/images/Campaign_Editor_2.png">
		<label style="font-size: 12px;" for="Campaign_Editor_2">
			<br>
			<b>Figure 2</b>: Campaign Stage Editor, note the new fields: *data_dispatcher_dataset_query* and *data_dispatcher_project_id*
		</label>
	</center>