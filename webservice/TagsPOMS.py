#!/usr/bin/env python

# This module contain the methods that handle the
# List of methods:  link_tags, delete_campaigns_tags, search_tags, auto_complete_tags_search, auto_complete_tags_search
# Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel,
# Stephen White and Michael Gueith.
### November, 2016.


from collections import deque
from sqlalchemy import desc
from .poms_model import CampaignStage, Campaign, Tag, CampaignsTag


class TagsPOMS:
    # h3. __init__
    def __init__(self, ps):
        self.poms_service = ps

    # h3. link_tags
    def link_tags(self, ctx, campaign_id, tag_name):
        tag = ctx.db.query(Tag).filter(Tag.tag_name == tag_name, Tag.experiment == ctx.experiment).first()

        if not tag:  # we do not have a tag in the db for this experiment so create the tag and then do the linking
            tag = Tag()
            tag.tag_name = tag_name
            tag.experiment = ctx.experiment
            tag.creator = ctx.get_experimenter().experimenter_id
            tag.creator_role = ctx.role
            ctx.db.add(tag)

        # we have a tag in the db for this experiment so go ahead and do
        # the linking
        campaign_ids = str(campaign_id).split(",")
        msg = "OK"
        for cid in campaign_ids:
            camp = ctx.db.query(Campaign).filter(Campaign.campaign_id == cid).scalar()
            camp.tags.append(tag)
            ctx.db.add(camp)
        ctx.db.commit()
        response = {"campaign_id": campaign_id, "tag_id": tag.tag_id, "tag_name": tag.tag_name, "msg": msg}
        return response

    # h3. search_campaigns
    def search_campaigns(self, ctx, search_term):
        query = (
            ctx.db.query(Campaign, CampaignStage)  #
            .filter(
                CampaignStage.campaign_id == Campaign.campaign_id,
                Campaign.name.like(search_term),
                Campaign.experiment == ctx.experiment,
            )
            .order_by(Campaign.campaign_id, CampaignStage.campaign_stage_id)
        )
        results = query.all()
        return results

    # h3. search_tags
    def search_tags(self, ctx, tag_name):
        q = ctx.db.query(Tag).filter(Tag.tag_name.like(tag_name), Tag.experiment == ctx.experiment).order_by(Tag.tag_name)
        tags = q.all()
        results = [(tag, [c for c in tag.campaigns.order_by(Campaign.name).all()]) for tag in tags]
        return results

    # h3. delete_tag_entirely
    def delete_tag_entirely(self, ctx, ses_get, tag_id):
        tag = ctx.db.query(Tag).filter(Tag.tag_id == tag_id).first()
        if ses_get("experimenter").is_authorized(tag.experiment):
            ctx.db.query(CampaignsTag).filter(CampaignsTag.tag_id == tag_id).delete(synchronize_session=False)
            ctx.db.query(Tag).filter(Tag.tag_id == tag_id).delete(synchronize_session=False)
            ctx.db.commit()
            response = {"msg": "OK"}
        else:
            response = {"msg": "You are not authorized to delete tags."}
        return response

    # h3. delete_campaigns_tags
    def delete_campaigns_tags(self, ctx, campaign_id, tag_id, delete_unused_tag=False):
        response = {"msg": "OK"}
        campaign_ids = str(campaign_id).split(",")
        for cid in campaign_ids:
            (ctx.db.query(CampaignsTag).filter(CampaignsTag.campaign_id == cid, CampaignsTag.tag_id == tag_id)).delete(  #
                synchronize_session=False
            )
            if delete_unused_tag:
                # If the tag is not used, delete it.
                ct = ctx.db.query(CampaignsTag).filter(CampaignsTag.tag_id == tag_id).first()
                print("ct: %s" % type(ct))
                if ct is None:
                    ctx.db.query(Tag).filter(Tag.tag_id == tag_id).delete(synchronize_session=False)
        ctx.db.commit()
        return response

    # h3. search_all_tags
    def search_all_tags(self, ctx, cl):
        cids = cl.split(",")  # Campaign IDs list
        result = (ctx.db.query(Tag).filter(Tag.campaigns.any(Campaign.campaign_id.in_(cids))).order_by(Tag.tag_name)).all()  #
        retval = []
        for r in result:
            retval.append([r.tag_id, r.tag_name])
        response = {"result": retval, "msg": "OK"}
        return response

    # h3. auto_complete_tags_search
    def auto_complete_tags_search(self, ctx, q):
        q.replace("*", "%")  # So the unix folks are happy
        response = {}
        results = deque()
        rows = (
            ctx.db.query(Tag)  #
            .filter(Tag.tag_name.like("%" + q + "%"), Tag.experiment == ctx.experiment)
            .order_by(desc(Tag.tag_name))
        ).all()
        for row in rows:
            results.append({"name": row.tag_name})
        response["results"] = list(results)
        return response
