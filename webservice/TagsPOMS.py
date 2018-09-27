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
from .poms_model import CampaignStage,  Campaign, Submission, Tag, CampaignsTag
from . import logit
from datetime import datetime, tzinfo, timedelta
from .utc import utc


class TagsPOMS(object):

    def __init__(self, ps):
        self.poms_service = ps

    def link_tags(self, dbhandle, ses_get, campaign_id, tag_name, experiment):
        if ses_get('experimenter').session_experiment == experiment:
            tag = dbhandle.query(Tag).filter(Tag.tag_name == tag_name, Tag.experiment == experiment).first()
            if not tag:  # we do not have a tag in the db for this experiment so create the tag and then do the linking
                tag = Tag()
                tag.tag_name = tag_name
                tag.experiment = experiment
                tag.creator = ses_get('experimenter').experimenter_id
                tag.creator_role = ses_get('experimenter').session_role
                dbhandle.add(tag)
            # we have a tag in the db for this experiment so go ahead and do the linking
            campaign_ids = str(campaign_id).split(',')
            msg = "OK"
            for cid in campaign_ids:
                camp = dbhandle.query(Campaign).filter(Campaign.campaign_id == cid).scalar()
                camp.tags.append(tag)
                dbhandle.add(camp)
            dbhandle.commit()
            response = {"campaign_id": campaign_id, "tag_id": tag.tag_id, "tag_name": tag.tag_name, "msg": msg}
            return response
        else:
            response = {"msg": "You are not authorized to tag campaigns."}
            return response


    def search_campaigns(self, dbhandle, search_term):
        query = (dbhandle.query(Campaign, CampaignStage)
                 .filter(CampaignStage.campaign_id == Campaign.campaign_id, Campaign.name.like(search_term))
                 .order_by(Campaign.campaign_id, CampaignStage.campaign_stage_id))
        results = query.all()
        return results


    def search_tags(self, dbhandle, tag_name):
        q = dbhandle.query(Tag).filter(Tag.tag_name.like(tag_name)).order_by(Tag.tag_name)
        tags = q.all()
        results = [(tag, [c for c in tag.campaigns.order_by(Campaign.name).all()]) for tag in tags]
        return results


    def delete_tag_entirely(self, dbhandle, ses_get, tag_id):
        tag = dbhandle.query(Tag).filter(Tag.tag_id == tag_id).first()
        if ses_get('experimenter').is_authorized(tag.experiment):
            dbhandle.query(CampaignsTag).filter(CampaignsTag.tag_id == tag_id).delete(synchronize_session=False)
            dbhandle.query(Tag).filter(Tag.tag_id == tag_id).delete(synchronize_session=False)
            dbhandle.commit()
            response = {"msg": "OK"}
        else:
            response = {"msg": "You are not authorized to delete tags."}
        return response

    def delete_campaigns_tags(self, dbhandle, ses_get, campaign_id, tag_id, experiment, delete_unused_tag=False):
        response = {"msg": "OK"}
        if ses_get('experimenter').session_experiment == experiment:
            campaign_ids = str(campaign_id).split(',')
            for cid in campaign_ids:
                (dbhandle.query(CampaignsTag)
                 .filter(CampaignsTag.campaign_id == cid, CampaignsTag.tag_id == tag_id)
                ).delete(synchronize_session=False)
                if delete_unused_tag:
                    # If the tag is not used, delete it.
                    ct = dbhandle.query(CampaignsTag).filter(CampaignsTag.tag_id == tag_id).first()
                    print("ct: %s" % type(ct))
                    if ct is None:
                        dbhandle.query(Tag).filter(Tag.tag_id == tag_id).delete(synchronize_session=False)
            dbhandle.commit()
        else:
            response = {"msg": "You are not authorized to delete tags."}
        return response

    def search_all_tags(self, dbhandle, cl):
        cids = cl.split(',')        # Campaign IDs list
        result = (dbhandle.query(Tag)
                  .filter(Tag.campaigns.any(Campaign.campaign_id.in_(cids)))
                  .order_by(Tag.tag_name)
                 ).all()
        retval = []
        for r in result:
            retval.append([r.tag_id, r.tag_name])
        response = {"result": retval, "msg": "OK"}
        return response


    def auto_complete_tags_search(self, dbhandle, experiment, q):
        q.replace('*', '%')  #So the unix folks are happy
        response = {}
        results = deque()
        rows = (dbhandle.query(Tag)
                .filter(Tag.tag_name.like('%' + q + '%'), Tag.experiment == experiment)
                .order_by(desc(Tag.tag_name))
               ).all()
        for row in rows:
            results.append({"name": row.tag_name})
        response["results"] = list(results)
        return response
