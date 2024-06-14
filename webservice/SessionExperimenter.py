#!/usr/bin/env python
from collections import deque

import sys
import os
import socket
from datetime import datetime

class SessionExperimenter:

    def __init__(self, experimenter_id=None, first_name=None, last_name=None,
                 username=None, authorization=None, session_experiment=None, session_role=None, root=None):
        self.experimenter_id = experimenter_id
        self.first_name = first_name
        self.last_name = last_name
        self.username = username
        self.authorization = authorization
        self.session_role = session_role
        self.session_experiment = session_experiment
        self.root = root or False

    def get_allowed_roles(self):
        """
        Returns the list of allowed roles for the user/experiment in the session
        """
        exp = self.authorization.get(self.session_experiment, {'roles': []})
        return exp.get('roles')

    def is_authorized(self, campaign=None):
        """
        Who can change a campagin/any object with properties fo experiment, creator and creator_role :
                The creator can change her/his own campaign_stages with the same role used to create the campaign.
                The root can change any campaign_stages.
                The superuser can change any campaign_stages that in the same experiment as the superuser.
                Anyone with a production role can change a campaign created with a production role.
        :param campaign: Name of the campaign.
        :return: True or False
        """
        if not campaign:
            return False
        if self.is_root():
            return True
        elif self.is_superuser() and self.session_experiment == campaign.experiment:
            return True
        elif self.is_production and self.session_experiment == campaign.experiment \
                and campaign.creator_role == "production":
            return True
        elif campaign.creator == self.experimenter_id and campaign.experiment == self.session_experiment \
                and campaign.creator_role == self.session_role:
            return True
        else:
            return False

    def is_root(self):
        return self.root

    def is_superuser(self):
        if self.session_role == 'superuser':
            return True
        return False

    def is_production(self):
        if self.session_role == "production":
            return True
        return False

    def is_analysis(self):
        if self.session_role == "analysis":
            return True
        return False

    def user_authorization(self):
        """
        Returns a dictionary of dictionaries.  Where:
          {'experiment':
            {'roles': [analysis,production,superuser,root]   # Ordered list of roles the user plays in the experiment
            },
          }
        """
        return self.authorization

    def roles(self):
        """
        Returns a list of roles for this user/experiment
        """
        return self.authorization.get(self.session_experiment).get('role')

    def __str__(self):
        return "%s %s %s" % (self.first_name, self.last_name, self.username)
