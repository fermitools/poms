### Overview

The Campaign Editor referred to in several places in the POMS Overview makes use of a dynamic visualization library called [vis.js](http://visjs.org/).  
Of the several graphic libraries _vis.js_ offers, the Campaign Editor is based on the _Network_ component which allows the user to create the whole campaign structure in a easy-to-use way employing the concept of '_node_' and '_edge_'.  
For our general purpose, the following terminology mapping is used:

* Node --> campaign defaults, campaign stage, login/setup and job type.
* Edge --> dependencies

To demonstrate the use of the Campaign Editor we will use the basic campaign '_eve_calib_' .  
The campaign has been created using the following basic steps:

* From POMS main menu, under _Campaign Data_ click on _Campaigns_
* From Campaigns page click on Add, assign a name and Save (in our example eve_calib).
* Under Campaigns, find the newly added campaign, _eve_calib_, and click on the 'GUI Editor' icon.

The following page shows the result of the previous actions:

![eve_calib_basic.png](/docs/images/eve_calib_basic.png)

The picture shows the Campaign Editor _drawing board_ showing the basic elements resulted from creating the campaign.  
The _drawing board_ has an 'Edit' button which will show available actions on the elements:

* Add Edge
* Delete Selected (only shown when an existing element is selected)

It is divided in two sections, the top part where you compose the campaign by adding stages and dependencies, and the bottom part where it shows the login/setup and jobtype elements.

When the campaign is created, it is assigned some default values and a single stage based on a generic login/setup and jobtype templates.  
The following picture shows all the forms 'behind' each element which are viewable by double clicking on each element.

![eve_calib_defaults.png](/docs/images/eve_calib_defaults.png)

The picture illustrates how the stage has inherited the defaults for the campaign; by convention, the default values in a stage are shown with an 'aqua' color background.  
The last two fields in the default and the stage show the assigned login setup and jobtype.

### Interacting with the GUI

The GUI offers easy way to add and modify the elements. Let's briefly describe some.  
Basic actions are as follows (mouse based):

* Double left click on an element, example the node eve_calib, will open up a form with the currently stored values which are modifiable.
* Single left click on an element will select it and allow actions on it chosen from the Edit bar.
* Single right click on the node will allow to create another node which will be a child of it created with the campaign default field values.

After opening a box with the information, you can click on it to move it around on the screen as to not cover other objects.  
One **important** point to make right away is that all the changes you will make on the Campaign Editor drawing board will **not** be saved until you click the **Save** button.  
Let's see the actions in practice as we add another stage and a dependency to our basic campaign.

Right click on stage0, a box will appear where you will assign the name to the new stage, in our case, stage1.  
As you can see from the picture, adding the new stage automatically creates a dependency between the '_parent_' stage and the '_child_'.

![eve_calib_add_stage_before_after.png](/docs/images/eve_calib_add_stage_before_after.png)

The following picture shows how the definitions for the new stage have been also inherited from the defaults and it also shows the content of the dependency.  
To view those, double click on the elements.

![eve_calib_stage1_def_dep.png](/docs/images/eve_calib_stage1_def_dep.png)

You can change the stage definition as you needed: double click on the stage and you can update which ever field. In the example below , the _completion percentage_ has been change.  
The picture shows how, after the change, the field background color is no longer _aqua_ to visually show that the value is different from the initial default.

![eve_calib_change_def.png](/docs/images/eve_calib_change_def.png)

A useful feature: if you change the value of a field in the default values, the change will be propagated to all the stages that use that default field: to do this, change the value in the default, _Save_ and then you will see the changes propagated.

Another interesting point regards changing the login/setup or the Job Type. It is typical that different campaign share the same login setup and/or, for a similar stage, the same job type.  
However, for example, if you need to change a field in a job type, you can do that as long as you provide a new name. In this case a new job type will be created automatically.  
See the picture below and what happens if you make changes but you don't change the Job Type name:

![eve_calib_change_jtype_before_after.png](/docs/images/eve_calib_change_jtype_before_after.png)

Another illustration of changes is in the next picture based on the following steps:

* We added another stage, stage2
* From the pull down menu we picked a new job type, generic_fife_process1
* We want to change the file pattern, so we change the name to generic_fife_process2

When we click OK on the job type box, the Jobtype Update box appears to confirm for which stages you want to apply the change.

![eve_calib_change_stage2.png](/docs/images/eve_calib_change_stage2.png)

When we confirm and save the new campaign will have the following configuration:

![eve_calib_change_stage2_done.png](/docs/images/eve_calib_change_stage2_done.png)

With the simple actions described above you can create and modify campaigns as you need.

An interesting situation is when you want to add multiple _children_ from the same parent, example 3 stages from the existing stage1: this can be accomplished by right clicking on parent and when specifying the name add '*N', N being the number of stages you want to create; as usual the children will inherit all the fields from the defaults.  
See below where 3 stages were created:

![eve_calib_add_3children.png](/docs/images/eve_calib_add_3children.png)

You can make campaigns as complicated as you wish. The following pictures shows another case:

![mvi_split_merge.png](/docs/images/mvi_split_merge.png)

