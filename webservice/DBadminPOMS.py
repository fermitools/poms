#!/usr/bin/env python

'''
This module contain the methods that allow to modify the data.
List of methods: user_edit, experiment_members, experiment edit, experiment_authorize.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: September 30, 2016.
'''


from model.poms_model import Experimenter, Experiment, ExperimentsExperimenters

class DBadminPOMS:
    #def user_edit(self, db = cherrypy.request.db,email = None, action = None, *args, **kwargs):
    def user_edit(self, dbhandle, *args, **kwargs):
        message = None
        data = {}
        email = kwargs.pop('email',None)
        action = kwargs.pop('action',None)

        if action == 'membership':
            # To update memberships set all the tags to false and then reset the needed ones to true.
            e_id = kwargs.pop('experimenter_id',None)
            dbhandle.query(ExperimentsExperimenters).filter(ExperimentsExperimenters.experimenter_id==e_id).update({"active":False})
            for key,exp in kwargs.items():
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
            if dbhandle.query(Experimenter).filter(Experimenter.email==email).one():
                message = "An experimenter with the email %s already exists" %  email
            else:
                experimenter = Experimenter()
                experimenter.first_name = kwargs.get('first_name')
                experimenter.last_name = kwargs.get('last_name')
                experimenter.email = email
                dbhandle.add( experimenter)
                dbhandle.commit()

        elif action == "edit":
            values = {    "first_name" : kwargs.get('first_name'),
                        "last_name"  : kwargs.get('last_name'),
                        "email"      : email    }
            dbhandle.query(Experimenter).filter(Experimenter.experimenter_id==kwargs.get('experimenter_id')).update(values)
            dbhandle.commit()

        if email:
            experimenter = dbhandle.query(Experimenter).filter(Experimenter.email == email ).first()
            if experimenter == None:
                message = "There is no experimenter with the email %s" % email
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


    def experiment_members(self, dbhandle, *args, **kwargs):
        exp = kwargs['experiment']
        query = (dbhandle.query(Experiment,ExperimentsExperimenters,Experimenter)
                .join(ExperimentsExperimenters).join(Experimenter)
                .filter(Experiment.name==exp)
                .order_by(ExperimentsExperimenters.active.desc(),Experimenter.last_name))
        trows=""
        for experiment, e2e, experimenter in query:
            active = "No"
            if e2e.active:
                active="Yes"
            trow = """<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>""" % (experimenter.first_name, experimenter.last_name, experimenter.email, active)
            trows = "%s%s" % (trows,trow)
        return (trows)


    def experiment_edit(self, dbhandle):
        return(dbhandle.query(Experiment).order_by(Experiment.experiment))


    def experiment_authorize(self, dbhandle, loghandle, *args, **kwargs):
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
                dbhandle.query(Experiment).filter(Experiment.experiment==experiment).delete()
            dbhandle.commit()
        except IntegrityError, e:
            message = "The experiment, %s, is used and may not be deleted." % experiment
            loghandle(e.message)
            dbhandle.rollback()
        except SQLAlchemyError, e:
            dbhandle.rollback()
            message = "SqlAlchemy error - %s" % e.message
            loghandle(e.message)
        return(message)
