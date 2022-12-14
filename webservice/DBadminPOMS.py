#!/usr/bin/env python

"""
This module contain the methods that allow to modify the raw database
List of methods: experiment_members, experiment edit, experiment_authorize.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version
   of functions in poms_service.py written by Stephen White.
Date: September 30, 2016.
"""

from webservice.Permissions import Permissions
from . import logit
from .poms_model import Experiment, Experimenter, ExperimentsExperimenters


class DBadminPOMS:
    """
        generic auto generated database edit/admin screens
    """
    def __init__(self):
        self.permissions = Permissions()

    # h3. experiment_membership
    def experiment_membership(self, ctx):
        """
            return members of experiment
        """
        members = (
            ctx.db.query(Experiment, ExperimentsExperimenters, Experimenter)  #
            .join(ExperimentsExperimenters.experiment_obj)
            .join(ExperimentsExperimenters.experimenter_obj)
            .filter(Experiment.experiment == ctx.experiment)
            .filter(ExperimentsExperimenters.active.is_(True))
            .order_by(ExperimentsExperimenters.active.desc(), Experimenter.last_name)
        ).all()
        data = {"members": members}
        return data

    def update_experiment_shifters(self, ctx, **kwargs):
        logit.log("update_experiment_shifters: %s" % repr(kwargs))
        members = self.experiment_membership(ctx)["members"]
        for m in members:
            u = m.Experimenter.username
            r = m.ExperimentsExperimenters.role
            if u in kwargs:
                if kwargs[u] == "production-shifter" and r == "analysis":
                    m.ExperimentsExperimenters.role = "production-shifter"
                    logit.log("update_experiment_shifters: adding shifter to %s" % u)
                    ctx.db.add(m.ExperimentsExperimenters)
                # if kwargs[u] == "production-shifter" and r == "production-shifter":
                #   logit.log("update_experiment_shifters: user %s already a shifter" % u)
            else:
                if r == "production-shifter":
                    logit.log("update_experiment_shifters: dropping shifter from %s" % u)
                    m.ExperimentsExperimenters.role = "analysis"
                    ctx.db.add(m.ExperimentsExperimenters)
                # if r == "analysis":
                #   logit.log("update_experiment_shifters: user %s already not a shifter" % u)

        ctx.db.commit()
        # clear permissions cache since we updated permissions.
        self.permissions.clear_cache()
        return "Ok"
