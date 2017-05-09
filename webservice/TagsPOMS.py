#!/usr/bin/env python

### This module contain the methods that handle the
### List of methods:  link_tags, delete_campaigns_tags, search_tags, auto_complete_tags_search, auto_complete_tags_search
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### November, 2016.


from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import func, desc
from poms.model.poms_model import Service, ServiceDowntime, Experimenter, Experiment, ExperimentsExperimenters, Job, JobHistory, Task, CampaignDefinition, TaskHistory, Campaign, LaunchTemplate, Tag, CampaignsTags, JobFile, CampaignSnapshot, CampaignDefinitionSnapshot,LaunchTemplateSnapshot,CampaignRecovery,RecoveryType, CampaignDependency
import json

class TagsPOMS():

        def __init__(self, ps):
            self.poms_service = ps

        def show_tags(self, dbhandle, experiment):
            tl = dbhandle.query(Tag).filter(Tag.experiment == experiment)
            return tl

        def link_tags(self, dbhandle, ses_get, campaign_id, tag_name, experiment):
            if ses_get.get('experimenter').is_authorized(experiment):
                response = {}
                tag = dbhandle.query(Tag).filter(Tag.tag_name == tag_name, Tag.experiment == experiment).first()
                if tag:  #we have a tag in the db for this experiment so go ahead and do the linking
                    try:
                        ct = CampaignsTags()
                        ct.campaign_id = campaign_id
                        ct.tag_id = tag.tag_id
                        dbhandle.add(ct)
                        dbhandle.commit()
                        response = {"campaign_id": ct.campaign_id, "tag_id": ct.tag_id, "tag_name": tag.tag_name, "msg": "OK"}
                        return response
                    except IntegrityError:
                        response = {"msg": "This tag already exists."}
                        return response
                else:  #we do not have a tag in the db for this experiment so create the tag and then do the linking
                    try:
                        t = Tag()
                        t.tag_name = tag_name
                        t.experiment = experiment
                        dbhandle.add(t)
                        dbhandle.commit()

                        ct = CampaignsTags()
                        ct.campaign_id = campaign_id
                        ct.tag_id = t.tag_id
                        dbhandle.add(ct)
                        dbhandle.commit()
                        response = {"campaign_id": ct.campaign_id, "tag_id": ct.tag_id, "tag_name": t.tag_name, "msg": "OK"}
                        return response
                    except IntegrityError:
                        response = {"msg": "This tag already exists."}
                        return response

            else:
                response = {"msg": "You are not authorized to add tags."}
                return response



        def delete_campaigns_tags(self, dbhandle, ses_get, campaign_id, tag_id, experiment):

            if ses_get('experimenter').is_authorized(experiment):
                dbhandle.query(CampaignsTags).filter(CampaignsTags.campaign_id == campaign_id, CampaignsTags.tag_id == tag_id).delete()
                dbhandle.commit()
                response = {"msg": "OK"}
            else:
                response = {"msg": "You are not authorized to delete tags."}
            return response


        def search_tags(self, dbhandle, q):

            q_list = q.split(" ")
            query = dbhandle.query(Campaign).filter(CampaignsTags.tag_id == Tag.tag_id, Tag.tag_name.in_(q_list), Campaign.campaign_id == CampaignsTags.campaign_id).group_by(Campaign.campaign_id).having(func.count(Campaign.campaign_id) == len(q_list))
            results = query.all()
            return results, q_list


        def auto_complete_tags_search(self, dbhandle, experiment, q):
            response = {}
            results = []
            rows = dbhandle.query(Tag).filter(Tag.tag_name.like('%'+q+'%'), Tag.experiment == experiment).order_by(desc(Tag.tag_name)).all()
            for row in rows:
                results.append({"tag_name": row.tag_name})
            response["results"] = results
            return json.dumps(response)
