---
layout: page
title: GUI Campaign Editor User Guide
---
* TOC
{:toc}

### Getting there

You can either:

* Edit an existing campaign
  * Go to the Campaigns/Tagged Campaigns page from the navigation bar/menu.
  * Pick the [Edit Campaign] button for one of the tagged campaigns
* Clone an existing campaign
  * Go to the Campaigns/Tagged Campaigns page from the navigation bar/menu.
  * Fill in from/to replacement strings (i.e MC9 -> MC10) for one of the tagged campaigns
  * Click the [Clone Campaign] button
* Clone a template/sample Campaign
  * Go to the Template Campaigns link in the navigation bar/menu (NOTE: not yet implemented)
  * fill in a replacement string for the "generic" in the campaign
  * Click the [Clone Campaign] button

You should now be at a screen that looks like:

![Campaign Editor]({{ site.url }}/docs/images/campaign_editor.png)

You have

* a "Default Values" box with values common accross your Campaign Stages.
  * you can click the [icon] box to see the form to edit the values,
  * and then click the [icon] box to hide the form again
* a diagram of the campaign stages with dependencies connecting them.
  * each campaign stage has an [grid icon] you click to toggle the view of the propreties edit form
  * each dependency link has an [grid icon] you click toggle the view of its properties edit form
* Above the diagram are buttons to
  * create a new stage in the campaign
  * link two stages with a dependency
* Notes on stages
  * stages are draggable so you can make room for new stages.
  * the edit form also has an [recyle icon] you click to delete that item
* There are also boxes for the campaign's
  * job type(s) and
  * login/launch templates

### Sample task tutorials

#### Adding a new campaign stage in the middle

* Go into the editor
* drag the campaign stages following where you want to add it over to make room
* remove the existing dependency
  * click the [grid icon] on the dependency line
  * click the [recylce icon] on that form
  * confirm the popup
* click "New Stage"
* Enter its name into the popup
* Click the previous stage to highlight it
* Click the new stage to highlight it
* Click on "Add dependency"
* Click the previous stage again to un-highlight it
* Click the following stage to highlight it
* Click the "Add dependency"
* Click Save

#### Changing the software version in the whole campaign

* Go into the editor
* click on the Default Values [grid icon]
* change the software version
* click the [grid icon] again to close the form
* Click Save
* you can click the individual stage [grid icon] to open/close the form to make sure they havent overridden it
