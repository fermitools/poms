---
title: "Release v4.0.0 Backward Compatibility"
---

<h3>What You need to know!</h3> 

As you probably have seen from the Release Notes there has been several changes in this new version.

## About Campaigns

The main _conceptual_ change regards the Campaign which translates in technical changes.

Before, the Campaign was an _*abstract*_ concept: a collection of stages which were or not dependent on each other.
People occasionally used to define a _Tag_ to group the stages based on their purpose or they used very *long*  _campaign stages names_ to indicate they were related to the same workflow; for example:

* exp_prod_mu_100-1257MeV_fixposcontained_fixangle_gen
* exp_prod_mu_100-1257MeV_fixposcontained_fixangle_g4
* exp_prod_mu_100-1257MeV_fixposcontained_fixangle_detsim
* exp_prod_mu_100-1257MeV_fixposcontained_fixangle_reco
* exp_prod_mu_100-1257MeV_fixposcontained_fixangle_ana

In the new release, the _Campaign_ has been introduced as a physical object which contains stages (one or more).
The following is what has been done for this transition:

* For a stage that was  standalone  and without a tag, a campaign has been created with the same name as the stage.

* For stages that were  standalone  *but* had a tag, a campaign has been created with the same name as the tag.
  * This has introduced a problem that has been worked on.

* For stages which depended on each other  *and*  had a tag, a campaign has been created with the same name as the tag.
* For stages which depended on each other *without* a tag, a campaign name has been chosen based on common part in the stage names.

As some problems have been discovered in this process, we will be working with the experiments to improve naming conventions.

One of the *very* positive aspects of having a campaign, is that you can confine the purpose of the stages in the name of the campaign and use much shorter names for the stages.
So, using the example above, we could have the following:

* Campaign Name:  exp_prod_mu_100-1257MeV_fixposcontained_fixangle
* Campaign Stages:
  * gen
  * g4
  * detsim
  * reco
  * ana

Since the stages belong to the campaign, you can use same stage names for convenience for another campaign, example:

* Campaign Name: exp_prod_mu_100-2500MeV_fixposcontained_fixangle
* Campaign Stages:
  * gen
  * g4
  * detsim
  * reco
  * ana

## About Tagging

The concept of  a _Tag_ still exists, however it is applied to the Campaign level, not to the stage level.
You can add/remove tags from the _Campaigns_ page. 

## About Campaign Editor

Since the campaign exists, when it is presented in the GUI Editor it will appear in a gray background oval shape object.
When the conversion was made, we could run into a couple of situations:

<a style="color:magenta;">Standalone stage</a>: the campaign was created with the same fields and values as the stage. 
At this point you can do the following:

* Modify the stage fields: the new values will belong to the modified stage, campaign _default_ values are *not* changed.
* Create a new stage: the new stage is created using the campaign _default_ values; when you change the new stage, again changes apply just to the stage, not default.

<a style="color:magenta;">Campaign with multiple stages</a>: the campaign was created using arbitrarly the _'most present'_ values among the stages.
Again you can proceed and apply the changes you need.

Please remember that the changes will be in effect ONLY after clicking the Save button.

## About Campaign Editor and field values from pull-down menu

Besides the stages, in the Campaign Editor drawing board you have the Login setup and the Job type. The ones you see are the one used in the stages.
If you decide to create a new login setup or job type opening a different TAB or window, and you still have the Campaign Editor with the Campaign opened, please remember to *refresh* the page in order to repopulate the pull down menu and in so doing see the new values.
 

