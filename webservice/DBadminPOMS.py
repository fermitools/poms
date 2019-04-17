#!/usr/bin/env python

"""
This module contain the methods that allow to modify the raw database
List of methods: experiment_members, experiment edit, experiment_authorize.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version
   of functions in poms_service.py written by Stephen White.
Date: September 30, 2016.
"""


#from . import logit
from .poms_model import Experiment, Experimenter, ExperimentsExperimenters

class DBadminPOMS:
    """
        generic auto generated database edit/admin screens
    """
    def experiment_membership(self, ctx):
        """
            return members of experiment
        """
        members = (ctx.db.query(Experiment, ExperimentsExperimenters, Experimenter) #
                   .join(ExperimentsExperimenters.experiment_obj)
                   .join(ExperimentsExperimenters.experimenter_obj)
                   .filter(Experiment.experiment == ctx.experiment)
                   .filter(ExperimentsExperimenters.active.is_(True))
                   .order_by(ExperimentsExperimenters.active.desc(), Experimenter.last_name)
                   ).all()
        data = {'members': members}
        return data
