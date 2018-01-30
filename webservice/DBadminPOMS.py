#!/usr/bin/env python

'''
This module contain the methods that allow to modify the raw database
List of methods: user_edit, experiment_members, experiment edit, experiment_authorize.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Stephen White.
Date: September 30, 2016.
'''

from collections import deque
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from . import logit
from .poms_model import Experimenter, Experiment, ExperimentsExperimenters
from sqlalchemy.orm.exc import NoResultFound

class DBadminPOMS:
    def user_edit(self, dbhandle, *args, **kwargs):
        '''
            callback from user edit page
        '''
        message = None
        data = {}
        username = kwargs.pop('username',None)
        action = kwargs.pop('action',None)

        if action == 'membership':
            # To update memberships set all the tags to false and then reset the needed ones to true.
            e_id = kwargs.pop('experimenter_id',None)
            dbhandle.query(ExperimentsExperimenters).filter(ExperimentsExperimenters.experimenter_id==e_id).update({"active":False})
            for key,exp in list(kwargs.items()):
                updated = (
                            dbhandle.query(ExperimentsExperimenters)
                            .filter(ExperimentsExperimenters.experimenter_id==e_id)
                            .filter(ExperimentsExperimenters.experiment==exp).update({"active":True})
                            )
                if updated==0:
                    EE = ExperimentsExperimenters()
                    EE.experimenter_id = e_id
                    EE.experiment = exp
                    EE.active = True
                    dbhandle.add( EE )
            dbhandle.commit()

        elif action == "add":
            if dbhandle.query(Experimenter).filter(Experimenter.username==username).first():
                message = "An experimenter with the username %s already exists" %  username
            else:
                experimenter = Experimenter()
                experimenter.first_name = kwargs.get('first_name')
                experimenter.last_name = kwargs.get('last_name')
                experimenter.username = username
                experimenter.session_experiment = ''
                experimenter.session_role = 'analysis'
                dbhandle.add( experimenter)
                dbhandle.commit()

        elif action == "edit":
            values = {    "first_name" : kwargs.get('first_name'),
                        "last_name"  : kwargs.get('last_name'),
                        "username"      : username    }
            dbhandle.query(Experimenter).filter(Experimenter.experimenter_id==kwargs.get('experimenter_id')).update(values)
            dbhandle.commit()

        if username:
            experimenter = dbhandle.query(Experimenter).filter(Experimenter.username==username).first()
            if experimenter == None:
                message = "There is no experimenter with the username %s" % username
            else:
                data['experimenter'] = experimenter
                # Experiments that are members of an experiment can be active or inactive
                data['member_of_exp'] = dbhandle.query(ExperimentsExperimenters).filter(ExperimentsExperimenters.experimenter_id == experimenter.experimenter_id)
                # Experimenters that were never a member of an experiment will not have an entry in the experiments_experimenters table for that experiment
                subquery = dbhandle.query(ExperimentsExperimenters.experiment).filter(ExperimentsExperimenters.experimenter_id == experimenter.experimenter_id)
                data['not_member_of_exp'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(subquery))

        dbhandle.commit()
        data['message'] = message
        return data


    def experiment_members(self, dbhandle, experiment, *args, **kwargs):
        '''
            return members of experiment
        '''
        query = list(dbhandle.query(Experiment, ExperimentsExperimenters, Experimenter)
                    .join(ExperimentsExperimenters).join(Experimenter)
                    .filter(Experiment.name==experiment)
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
        return (trows)


    def member_experiments(self, dbhandle, username, *args, **kwargs):
        '''
            return experiments a given user is a member of
        '''

        subq = (dbhandle.query(ExperimentsExperimenters, Experimenter.username, Experimenter.first_name, Experimenter.last_name)
                    .join(Experimenter, Experimenter.experimenter_id==ExperimentsExperimenters.experimenter_id)
                    .filter(Experimenter.username==username)
                )
        #~ return '{}'.format('\n'.join(map(str, subq)))   # DEBUG

        subq = subq.subquery()
        query = (dbhandle.query(Experiment, subq)
                    .join(subq, subq.c.experiment==Experiment.experiment, isouter=True)
                    .order_by(Experiment.experiment)
                )

# (Experiment, experimenter_id, experiment, active, username, first_name, last_name)
        trows = deque()
        for (experiment, experimenter_id, exp, active, username, first_name, last_name) in query:
            #~ trow = "{}\t{}\t{}\t{}\t{}\n".format(experiment.experiment, first_name, last_name, username, 'Active' if active else 'No')
            trows.append((experiment.experiment, first_name, last_name, username, active and 'Active'))
        return trows

    def experiment_edit(self, dbhandle):
        '''
           Info for experiment_edit template page.
           returns sorted experiment list
        '''
        return(dbhandle.query(Experiment).order_by(Experiment.experiment))


    def experiment_authorize(self, dbhandle, *args, **kwargs):
        '''
            add/delete experiments
        '''
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
        return(message)
