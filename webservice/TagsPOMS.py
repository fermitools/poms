#!/usr/bin/env python

### This module contain the methods that handle the
### List of methods:  link_tags, delete_campaigns_tags, search_tags, auto_complete_tags_search, auto_complete_tags_search
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel,
### Stephen White and Michael Gueith.
### November, 2016.


from collections import deque
import json
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, desc
from .poms_model import CampaignStage,  Campaign, Submission
from . import logit
from datetime import datetime, tzinfo, timedelta
from .utc import utc


class TagsPOMS(object):

    def __init__(self, ps):
        self.poms_service = ps


    def show_campaigns(self, dbhandle, experimenter, *args, **kwargs):
        action = kwargs.get('action', None)
        msg = "OK"
        if action == 'delete':
            campaign_id = kwargs.get('del_campaign_id')
            campaign = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
            if experimenter.is_authorized(campaign):
                subs = dbhandle.query(Submission).join(CampaignStage, Submission.campaign_stage_id == CampaignStage.campaign_stage_id).filter(CampaignStage.campaign_id == campaign_id)
                if subs.count() > 0:
                    msg = "This campaign has been submitted.  It cannot be deleted."
                else:
                    dbhandle.query(CampaignStage).filter(CampaignStage.campaign_id == campaign_id).delete(synchronize_session=False)
                    dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).delete(synchronize_session=False)
                    dbhandle.commit()
                    msg = "Campaign named %s with campaign_id %s and related CampagnStages were deleted ." % (kwargs.get('del_campaign_name'), campaign_id)
            else:
                msg = "You are not authorized to delete campaigns."

        tl = dbhandle.query(Campaign).filter(Campaign.experiment == experimenter.session_experiment).all()
        if not tl:
            return tl, "", msg
        last_activity_l = dbhandle.query(func.max(Submission.updated)).join(CampaignStage,Submission.campaign_stage_id == CampaignStage.campaign_stage_id).join(Campaign,CampaignStage.campaign_id == Campaign.campaign_id).filter(Campaign.experiment == experimenter.session_experiment).first()
        logit.log("got last_activity_l %s" % repr(last_activity_l))
        last_activity = ""
        if last_activity_l and last_activity_l and last_activity_l[0]:
            if datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
                last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        logit.log("after: last_activity %s" % repr(last_activity))
        return tl, last_activity, msg


    def link_tags(self, dbhandle, ses_get, campaign_stage_id, campaign_name, experiment):
        # if ses_get('experimenter').is_authorized(experiment): #FIXME
        # Fake it for now, we need to discuss who can manipulate campaigns.
        if ses_get('experimenter').session_experiment == experiment:
            camp = dbhandle.query(Campaign).filter(Campaign.name == campaig_name, Campaign.experiment == experiment).first()
            if not camp:  # we do not have a campaign in the db for this experiment so create the campaign and then do the linking
                camp = Campaign()
                camp.name = tag_name
                camp.experiment = experiment
                camp.creator = ses_get('experimenter').experimenter_id
                camp.creator_role = ses_get('experimenter').session_role
                dbhandle.add(camp)
            # we have a tag in the db for this experiment so go ahead and do the linking
            campaign_stage_ids = str(campaign_stage_id).split(',')
            msg = "OK"
            for cid in campaign_ids:
                cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == cid).first()
                cs.campaign_id = camp.campaign_id
                dbhandle.add(cs)
            dbhandle.commit()
            response = {"campaign_stage_id": campaign_stage_id, "campaign_id": ct.campaign_id, "tag_name": tag.tag_name, "msg": msg}
            return response
        else:
            response = {"msg": "You are not authorized to add campaigns."}
            return response

    def search_tags(self, dbhandle, search_term):
        query = (dbhandle.query(Campaign, CampaignStage)
                 .filter(CampaignStage.campaign_id == Campaign.campaign_id,
                         Campaign.tag_name.like(search_term),
                         CampaignStage.campaign_stage_id == CampaignStage.campaign_stage_id)
                 .order_by(Campaign.campaign_id, CampaignStage.campaign_stage_id))
        results = query.all()
        return results


    def search_all_tags(self, dbhandle, cl):

        cids = cl.split(',')        # CampaignStage IDs list
        # result = dbhandle.query(CampaignStage.campaign_obj.name).filter(Campaignstage.campaign_stage_id.in_(cids)).distinct()
        #
        # SELECT distinct(campaigns.tag_name)
        # FROM campaign_campaign_stages JOIN campaigns ON campaigns.campaign_id=campaign_campaign_stages.campaign_id
        # WHERE campaign_campaign_stages.campaign_stage_id in (513,514);
        #
        result = (dbhandle.query(Campaign.campaign_id, Campaign.tag_name)
                  .join(CampaignStage)
                  .filter(Campaign.campaign_id == CampaignStage.campaign_id)
                  .filter(CampaignStage.campaign_stage_id.in_(cids))
                  .distinct().all())
        # result = [(r[0], r[1]) for r in result]
        result = [tuple(r) for r in result]
        response = {"result": result, "msg": "OK"}
        return response


    def auto_complete_tags_search(self, dbhandle, experiment, q):
        response = {}
        results = deque()
        rows = dbhandle.query(Campaign).filter(Campaign.tag_name.like('%' + q + '%'), Campaign.experiment == experiment).order_by(desc(Campaign.tag_name)).all()
        for row in rows:
            results.append({"tag_name": row.tag_name})
        response["results"] = list(results)
        return response
