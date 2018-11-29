#!/usr/bin/env python

"""
This module contain the methods that allow to modify the raw database
List of methods: experiment_members, experiment edit, experiment_authorize.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Stephen White.
Date: September 30, 2016.
"""

from collections import deque

from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm.exc import NoResultFound

from . import logit
from .poms_model import Experiment, Experimenter, ExperimentsExperimenters


class DBadminPOMS:
    def experiment_membership(self, dbhandle, experiment, *args, **kwargs):
        """
            return members of experiment
        """
        members = (dbhandle.query(Experiment, ExperimentsExperimenters, Experimenter)
                   .join(ExperimentsExperimenters).join(Experimenter)
                   .filter(Experiment.name == experiment)
                   .filter(ExperimentsExperimenters.active == True)
                   .order_by(ExperimentsExperimenters.active.desc(), Experimenter.last_name)
                  ).all()
        data = {'members': members}
        return data
