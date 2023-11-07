---
layout: page
title: Analysis Use Proposal
---
* TOC
{:toc}
To let POMS be usable for Analysis users, we need:

* More permissions variations
  *  Users can be production/analysis users
  *  Analysis flag
    * Restricts what they see to their personal jobs/campaigns
    * Restricts what campaigns they can launch/kill
    * Restricts what roles they can use in campaigns(?)
    * Restricts what accounts they can ssh into to launch jobs.
* Campaigns need to be able to belong to individuals or to experiment-production
* Do we have subcategories of Production? (Calibration? etc.)

So this means campaigns need experiment and experimenter fields for ownership; Null experimenter means it is production?

Experimenter table need flags about whether they're production group or not; or maybe we add a more generic group membership mechanism? Then ownership would be a group; and users would all have a just-them group but they could also be in other groups (i.e. experiment-production)

Current experiment pull down (if they're in multiple exp) (which we've been discussing for simplifying existing forms, etc.) would become experiment/role pulldown; If a person were in the production group of two experiments, it would have 4 items 2 experiments x 2 roles... Or if we do groups it would just let them pick the group they're in that they want to operate as for now.

Need to figure out how analysis users get job launch credentials -- I don't think we get them all managed proxies from Grid Support folks, so how do we recommend they set things up?!? We need to be able to ssh into their account as poms/cd/pomsgpvm01.fnal.gov@FNAL.GOV and get their credential somehow, i.e. to launch recovery jobs.