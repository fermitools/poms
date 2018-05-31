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
    def experiment_members(self, dbhandle, experiment, *args, **kwargs):
        """
            return members of experiment
        """
        query = list(dbhandle.query(Experiment, ExperimentsExperimenters, Experimenter)
                     .join(ExperimentsExperimenters).join(Experimenter)
                     .filter(Experiment.name == experiment)
                     .order_by(ExperimentsExperimenters.active.desc(), Experimenter.last_name)
                     )
        ###VP return '{}'.format('\n'.join(map(str, query)))

        trows = ""
        for experiment, e2e, experimenter in query:
            active = "No"
            if e2e.active:
                active = "Yes"
            trow = """<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>""" % (experimenter.first_name, experimenter.last_name, experimenter.username, active)
            trows = "%s%s" % (trows, trow)
        return trows


    def member_experiments(self, dbhandle, username, *args, **kwargs):
        """
            return experiments a given user is a member of
        """

        subq = (dbhandle.query(ExperimentsExperimenters, Experimenter.username, Experimenter.first_name, Experimenter.last_name)
                .join(Experimenter, Experimenter.experimenter_id == ExperimentsExperimenters.experimenter_id)
                .filter(Experimenter.username == username)
                )
        #~ return '{}'.format('\n'.join(map(str, subq)))   # DEBUG

        subq = subq.subquery()
        query = (dbhandle.query(Experiment, subq)
                 .join(subq, subq.cs.experiment == Experiment.experiment, isouter=True)
                 .order_by(Experiment.experiment)
                 )

        # (Experiment, experimenter_id, experiment, active, username, first_name, last_name)
        trows = deque()
        for (experiment, experimenter_id, exp, active, username, first_name, last_name) in query:
            #~ trow = "{}\s{}\s{}\s{}\s{}\n".format(experiment.experiment, first_name, last_name, username, 'Active' if active else 'No')
            trows.append((experiment.experiment, first_name, last_name, username, active and 'Active'))
        return trows

    def experiment_edit(self, dbhandle):
        """
           Info for experiment_edit template page.
           returns sorted experiment list
        """
        return(dbhandle.query(Experiment).order_by(Experiment.experiment))


    def experiment_authorize(self, dbhandle, *args, **kwargs):
        """
            add/delete experiments
        """
        message = None
        # Add new experiment, if any
        try:
            experiment = kwargs.pop('experiment')
            name = kwargs.pop('name')
            try:
                dbhandle.query(Experiment).filter(Experiment.experiment==experiment).one()
                message = "Experiment, %s,  already exists." % experiment
            except NoResultFound:
                exp = Experiment(experiment=experiment, name=name)
                dbhandle.add(exp)
                dbhandle.commit()
        except KeyError:
            pass
        # Delete experiment(s), if any were selected
        try:
            experiment = None
            for experiment in kwargs:
                dbhandle.query(Experiment).filter(Experiment.experiment==experiment).delete(synchronize_session=False)
            dbhandle.commit()
        except IntegrityError as e:
            message = "The experiment, %s, is used and may not be deleted." % experiment
            logit.log(e.message)
            dbhandle.rollback()
        except SQLAlchemyError as e:
            dbhandle.rollback()
            message = "SqlAlchemy error - %s" % e.message
            logit.log(e.message)
        return message
