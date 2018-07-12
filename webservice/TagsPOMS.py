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
from .poms_model import CampaignStage, CampaignCampaignStages, Campaign, Submission
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
                subs = dbhandle.query(Submission).join(CampaignCampaignStages, Submission.campaign_stage_id == CampaignCampaignStages.campaign_stage_id).filter(CampaignCampaignStages.campaign_id == campaign_id)
                if subs.count() > 0:
                    msg = "This campaign has been submitted.  It cannot be deleted."
                else:
                    dbhandle.query(CampaignCampaignStages).filter(CampaignCampaignStages.campaign_id == campaign_id).delete(synchronize_session=False)
                    dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).delete(synchronize_session=False)
                    dbhandle.commit()
                    msg = "Campaign named %s with campaign_id %s was deleted ." % (kwargs.get('del_campaign_name'), campaign_id)
            else:
                msg = "You are not authorized to delete campaigns."

        tl = dbhandle.query(Campaign).filter(Campaign.experiment == experimenter.session_experiment).all()
        if not tl:
            return tl, "", msg
        last_activity_l = dbhandle.query(func.max(Submission.updated)).join(CampaignCampaignStages,Submission.campaign_stage_id == CampaignCampaignStages.campaign_stage_id).join(Campaign,CampaignCampaignStages.campaign_id == Campaign.campaign_id).filter(Campaign.experiment == experimenter.session_experiment).first()
        logit.log("got last_activity_l %s" % repr(last_activity_l))
        last_activity = ""
        if last_activity_l and last_activity_l and last_activity_l[0]:
            if datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
                last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        logit.log("after: last_activity %s" % repr(last_activity))
        return tl, last_activity, msg


    def link_tags(self, dbhandle, ses_get, campaign_stage_id, tag_name, experiment):
        # if ses_get('experimenter').is_authorized(experiment): #FIXME
        # Fake it for now, we need to discuss who can manipulate campaigns.
        if ses_get('experimenter').session_experiment == experiment:
            tag = dbhandle.query(Campaign).filter(Campaign.tag_name == tag_name, Campaign.experiment == experiment).first()
            if not tag:  # we do not have a tag in the db for this experiment so create the tag and then do the linking
                s = Campaign()
                s.tag_name = tag_name
                s.experiment = experiment
                s.creator = ses_get('experimenter').experimenter_id
                s.creator_role = ses_get('experimenter').session_role
                dbhandle.add(s)
                dbhandle.commit()
                tag = s
            # we have a tag in the db for this experiment so go ahead and do the linking
            campaign_ids = str(campaign_stage_id).split(',')
            msg = "OK"
            for cid in campaign_ids:
                try:
                    ct = CampaignCampaignStages()
                    ct.campaign_stage_id = cid
                    ct.campaign_id = tag.campaign_id
                    dbhandle.add(ct)
                    dbhandle.commit()
                except IntegrityError:
                    dbhandle.rollback()
                    msg = "This tag may already exist."
                    # response = {"msg": "Database error."}
                    # return response
            response = {"campaign_stage_id": campaign_stage_id, "campaign_id": ct.campaign_id, "tag_name": tag.tag_name, "msg": msg}
            return response
        else:
            response = {"msg": "You are not authorized to add campaigns."}
            return response

    def delete_campaigns_tags(self, dbhandle, ses_get, campaign_stage_id, campaign_id, experiment):

        if ',' not in str(campaign_stage_id):
            if ses_get('experimenter').is_authorized(experiment):
                dbhandle.query(CampaignCampaignStages).filter(CampaignCampaignStages.campaign_stage_id == campaign_stage_id, CampaignCampaignStages.campaign_id == campaign_id).delete(synchronize_session=False)
                dbhandle.commit()
                response = {"msg": "OK"}
            else:
                response = {"msg": "You are not authorized to delete campaigns."}
            return response
        else:
            # if ses_get('experimenter').is_authorized(experiment): FIXME
            if ses_get('experimenter').session_experiment == experiment:
                campaign_ids = str(campaign_stage_id).split(',')
                for cid in campaign_ids:
                    dbhandle.query(CampaignCampaignStages).filter(CampaignCampaignStages.campaign_stage_id == cid, CampaignCampaignStages.campaign_id == campaign_id).delete(synchronize_session=False)
                    dbhandle.commit()
                response = {"msg": "OK"}
            else:
                response = {"msg": "You are not authorized to delete campaigns."}
            return response


    def search_tags(self, dbhandle, search_term):
        query = (dbhandle.query(Campaign, CampaignStage)
                 .filter(CampaignCampaignStages.campaign_id == Campaign.campaign_id,
                         Campaign.tag_name.like(search_term),
                         CampaignStage.campaign_stage_id == CampaignCampaignStages.campaign_stage_id)
                 .order_by(Campaign.campaign_id, CampaignStage.campaign_stage_id))
        results = query.all()
        return results


    def search_all_tags(self, dbhandle, cl):

        cids = cl.split(',')        # CampaignStage IDs list
        # result = dbhandle.query(CampaignCampaignStages.tag_obj.tag_name).filter(CampaignCampaignStages.campaign_stage_id.in_(cids)).distinct()
        #
        # SELECT distinct(campaigns.tag_name)
        # FROM campaign_campaign_stages JOIN campaigns ON campaigns.campaign_id=campaign_campaign_stages.campaign_id
        # WHERE campaign_campaign_stages.campaign_stage_id in (513,514);
        #
        result = (dbhandle.query(Campaign.campaign_id, Campaign.tag_name)
                  .join(CampaignCampaignStages)
                  .filter(Campaign.campaign_id == CampaignCampaignStages.campaign_id)
                  .filter(CampaignCampaignStages.campaign_stage_id.in_(cids))
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
