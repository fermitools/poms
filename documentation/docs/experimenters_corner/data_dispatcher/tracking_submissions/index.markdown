---
layout: page
title: Tracking Submissions
---
* TOC
{:toc}

As highlighted earlier, real-time tracking of data dispatcher project submissions is facilitated through POMS. This capability is enabled via several avenues, including the newly integrated Data Dispatcher UI components found on campaign, stage, and submission details pages. Moreover, akin to SAM submissions, you can access submission statistics and review output files seamlessly. This feature amplifies transparency and enables a dynamic monitoring framework for all your data handling operations.

## Tracking via UI Elements

Transitioning to the data dispatcher framework has been designed to be intuitive for the experimenter. The idea is to ensure that the learning curve is minimal and you don't have to traverse a new learning trajectory just to adapt to this system. Here's a comforting thought: the familiarity in user interaction has been retained. The pages you've known retain their look, feel, and behavior as with SAM.
> * Tracking submissions? It's done on the same pages as before.
> * The file stats? They are located in the same areas, and on the same pages as before.
> * All the previous pages and UI elements? They continue to display the same types of data as before.

However, while the UI maintains a semblance of the former system, there are some subtle yet significant distinctions when you're operating under the data dispatcher as opposed to SAM:

### 1. File Status Definitions
* Unlike SAM, Data Dispatcher operates with a slightly different set of file statuses. Here are the key differences:
	* The "located" status isn't utilized in Data Dispatcher. Instead, a timeout mechanism is employed when fetching a file from the defined RSE (Replica Storage Element). If the file isn't found within the stipulated time, it's skipped and POMS (Production Operations Management System) tags it as "Unknown".
	* Leveraging the quick and easy state determination of files in a Data Dispatcher project, a "Submission % Completed" field is provided. 
		> This feature shines especially with split types, where a large project exists but only a portion of the files are to be processed. The field reflects the completion percentage of the specified submission, not the project in its entirety.
	* A file is marked as "initial" upon definition but prior to processing. POMS regards files in the "initial" state as "Not Submitted".
	* The status transitions to "Reserved" when Data Dispatcher fetches a file.
	* Post-processing, a file is tagged as "done" if the processing is successful.
	* Conversely, a failed processing attempt tags the file as "failed".
		* Additionally, a flag is associated with failed files to indicate whether a retry is feasible. However, the retry control does not reside with POMS.

	<center>
		<img name="file_stats_submission_details" src="{{ site.url }}/docs/images/file_stats_submission_details.png">
		<label style="font-size: 12px; margin-bottom: 20px;" for="file_stats_submission_details">
			<b>Figure 4</b>: Submission Details * File Stats: This picture gives an example of a new project that has not yet begun processing. Note that this project would likely fail because the files defined in the dataset do not include replicas * which is an indicator that they cannot be located in the designated RSE (Not shown).
			<br>
		</label>
	</center>
			
### 2. Campaign Stage Submissions Display Enhancements
* A new column has been introduced in the grid below to indicate the Data Handling Service utilized for the submission—either SAM or Data Dispatcher.
* The Status fields (both History and Current) have been updated to reflect one of three states: "Created", "Completed", or "Completed with failures". Unlike with SAM, POMS doesn't interface with FERRY to ascertain submission statuses when Data Dispatcher is in play. Instead, the real-time tracking of these statuses is enabled, as depicted in Figure 3.
	<center>
		<img name="campaign_stage_submissions" src="{{ site.url }}/docs/images/campaign_stage_submissions.png">
		<label style="font-size: 12px; margin-bottom: 20px;" for="campaign_stage_submissions">
			<b>Figure 5</b>: Campaign Stage Submissions: This picture gives an example campaign stage that had been submitted several times.
			<br>
		</label>
	</center>
			
### 3. Submission Relaunch with Specified Project ID
* Should you encounter failed files, wish to restart your project/run, or have any other reason to relaunch, this action can be accomplished via the 'Relaunch' button, as illustrated in Figure 6 below.
	* Initiating a campaign stage submission that is defined through a dataset query will engender a new data dispatcher project, encompassing the same files but in an 'initial' state.
	* An alternative method to simulate a "relaunch" is by updating the "data_dispatcher_project_id" field within the GUI editor, ensuring submissions are always tied to the specified project id.
	<center>
		<img name="submission_details" src="{{ site.url }}/docs/images/submission_details.png">
		<label style="font-size: 12px; margin-bottom: 20px;" for="submission_details">
			<b>Figure 6</b>: Displays the Submission Details UI of a Data Dispatcher Campaign
			<br>
		</label>
	</center>
			
### 4. Navigation to Metacat Webserver for File/Dataset Details
* Upon clicking the links provided in the file stats (refer to Figure 4), you will be directed to your Metacat webserver where detailed information regarding the file/dataset is available.
	<center>
		<img name="files-link-to-metacat" src="{{ site.url }}/docs/images/files-link-to-metacat.png">
		<label style="font-size: 12px; margin-bottom: 20px;" for="files-link-to-metacat">
			<b>Figure 6</b>: Metacat Webserver file details: Navigating here by clicking on any of the files returned in our file stat queries will display relevant metadata about the selected file.
			<br>
		</label>
	</center>