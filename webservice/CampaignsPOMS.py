#!/usr/bin/env python

'''
This module contain the methods that allow to create campaigns, definitions and templates.
List of methods:  launch_template_edit, campaign_definition_edit, campaign_edit, campaign_edit_query.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: September 30, 2016.
'''

from model.poms_model import Experiment
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from utc import utc


class CampaignsPOMS():

	def launch_template_edit(self, dbhandle, loghandle, seshandle, *args, **kwargs):
		data = {}
		message = None
		data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
		action = kwargs.pop('action',None)
		exp = kwargs.pop('experiment',None)
		if action == 'delete':
			name = kwargs.pop('name')
			try:
				dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).delete()
				dbhandle.commit()
			except Exception, e:
				message = "The launch template, %s, has been used and may not be deleted." % name
				loghandle(message)
				loghandle(e.message)
				dbhandle.rollback()

		if action == 'add' or action == 'edit':
			ae_launch_id = kwargs.pop('ae_launch_id')
			ae_launch_name = kwargs.pop('ae_launch_name')
			ae_launch_host = kwargs.pop('ae_launch_host')
			ae_launch_account = kwargs.pop('ae_launch_account')
			ae_launch_setup = kwargs.pop('ae_launch_setup')
			experimenter_id = kwargs.pop('experimenter_id')
			try:
				if action == 'add':
					template = LaunchTemplate(experiment=exp, name=ae_launch_name, launch_host=ae_launch_host, launch_account=ae_launch_account,
												launch_setup=ae_launch_setup,creator = experimenter_id, created = datetime.now(utc))
					dbhandle.add(template)
				else:
					columns = {
								"name":           ae_launch_name,
								"launch_host":    ae_launch_host,
								"launch_account": ae_launch_account,
								"launch_setup":   ae_launch_setup,
								"updated":        datetime.now(utc),
								"updater":        experimenter_id
								}
					template = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.launch_id==ae_launch_id).update(columns)
			except IntegrityError, e:
				message = "Integrity error - you are most likely using a name which already exists in database."
				loghandle(e.message)
				dbhandle.rollback()
			except SQLAlchemyError, e:
				message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
				loghandle(e.message)
				dbhandle.rollback()
			else:
				dbhandle.commit()

		# Find templates
		if exp: # cuz the default is find
			data['curr_experiment'] = exp
			data['authorized'] = seshandle('experimenter').is_authorized(exp)
			data['templates'] = dbhandle(LaunchTemplate,Experiment).join(Experiment).filter(LaunchTemplate.experiment==exp).order_by(LaunchTemplate.name)
		data['message'] = message
		return data


	def campaign_definition_edit(self, dbhandle, loghandle, seshandle, *args, **kwargs):
		data = {}
		message = None
		data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
		action = kwargs.pop('action',None)
		exp = kwargs.pop('experiment',None)
		if action == 'delete':
			name = kwargs.pop('name')
			cid = kwargs.pop('campaign_definition_id')
			try:
				dbhandle.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id==cid).delete()
				dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==cid).delete()
				dbhandle.commit()
			except Exception, e:
				message = "The campaign definition, %s, has been used and may not be deleted." % name
				loghandle(message)
				loghandle(e.message)
				loghandle.rollback()

		if action == 'add' or action == 'edit':
			campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
			name = kwargs.pop('ae_definition_name')
			input_files_per_job = kwargs.pop('ae_input_files_per_job')
			output_files_per_job = kwargs.pop('ae_output_files_per_job')
			output_file_patterns = kwargs.pop('ae_output_file_patterns')
			launch_script = kwargs.pop('ae_launch_script')
			definition_parameters = kwargs.pop('ae_definition_parameters')
			recoveries = kwargs.pop('ae_definition_recovery')
			experimenter_id = kwargs.pop('experimenter_id')
			try:
				if action == 'add':
					cd = CampaignDefinition( name=name, experiment=exp,
							input_files_per_job=input_files_per_job, output_files_per_job = output_files_per_job,
							output_file_patterns = output_file_patterns,
							launch_script=launch_script, definition_parameters=definition_parameters,
							creator=experimenter_id, created=datetime.now(utc))

					dbhandle.add(cd)
					dbhandle.flush()
					campaign_definition_id = cd.campaign_definition_id
				else:
					columns = {
								"name":                  name,
								"input_files_per_job":   input_files_per_job,
								"output_files_per_job":  output_files_per_job,
								"output_file_patterns":  output_file_patterns,
								"launch_script":         launch_script,
								"definition_parameters": definition_parameters,
								"updated":               datetime.now(utc),
								"updater":               experimenter_id
								}
					cd = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).update(columns)

			# now fixup recoveries -- clean out existing ones, and
			# add listed ones.
				dbhandle.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id == campaign_definition_id).delete()
				i = 0
				for rtn in json.loads(recoveries):
					rt = dbhandle.query(RecoveryType).filter(RecoveryType.name==rtn).first()
					cr = CampaignRecovery(campaign_definition_id = campaign_definition_id, recovery_order = i, recovery_type = rt)
					dbhandle.add(cr)
				dbhandle.commit()
			except IntegrityError, e:
				message = "Integrity error - you are most likely using a name which already exists in database."
				loghandle(e.message)
				dbhandle.rollback()
			except SQLAlchemyError, e:
				message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
				loghandle(e.message)
				dbhandle.rollback()
			else:
				dbhandle.commit()

		# Find definitions
		if exp: # cuz the default is find
			data['curr_experiment'] = exp
			data['authorized'] = seshandle('experimenter').is_authorized(exp)
			# for testing ui...
			#data['authorized'] = True
			data['definitions'] = (dbhandle.query(CampaignDefinition,Experiment)
									.join(Experiment)
									.filter(CampaignDefinition.experiment==exp)
									.order_by(CampaignDefinition.name)
									)
			# Build the recoveries for each campaign.
			cids = [row[0].campaign_definition_id for row in data['definitions'].all()]
			recs_dict = {}
			for cid in cids:
				recs = (dbhandle.query(CampaignRecovery).join(CampaignDefinition).options(joinedload(CampaignRecovery.recovery_type))
					.filter(CampaignRecovery.campaign_definition_id == cid,CampaignDefinition.experiment == exp)
					.order_by(CampaignRecovery.campaign_definition_id, CampaignRecovery.recovery_order))
				rec_list  = []
			for rec in recs:
				rec_list.append(rec.recovery_type.name )
			recs_dict[cid] = json.dumps(rec_list)
			data['recoveries'] = recs_dict
			data['rtypes'] = (dbhandle.query(RecoveryType.name,RecoveryType.description).order_by(RecoveryType.name).all())

		data['message'] = message
		return data


	def campaign_edit(self, dbhandle, loghandle, sesshandle, *args, **kwargs):
		data = {}
		message = None
		data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
		action = kwargs.pop('action',None)
		exp = kwargs.pop('experiment',None)
		if action == 'delete':
			campaign_id = kwargs.pop('campaign_id')
			name = kwargs.pop('name')
			try:
				dbhandle.query(CampaignDependency).filter(or_(CampaignDependency.needs_camp_id==campaign_id,
								CampaignDependency.uses_camp_id==campaign_id)).delete()
				dbhandle.query(Campaign).filter(Campaign.campaign_id==campaign_id).delete()
				dbhandle.commit()
			except Exception, e:
				message = "The campaign, %s, has been used and may not be deleted." % name
				loghandle(message)
				loghandle(e.message)
				dbhandle.rollback()

		if action == 'add' or action == 'edit':
			campaign_id = kwargs.pop('ae_campaign_id')
			name = kwargs.pop('ae_campaign_name')
			active = kwargs.pop('ae_active')
			split_type = kwargs.pop('ae_split_type')
			vo_role = kwargs.pop('ae_vo_role')
			software_version = kwargs.pop('ae_software_version')
			dataset = kwargs.pop('ae_dataset')
			param_overrides = kwargs.pop('ae_param_overrides')
			campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
			launch_id = kwargs.pop('ae_launch_id')
			experimenter_id = kwargs.pop('experimenter_id')
			depends = kwargs.pop('ae_depends')
			if depends and depends != "[]":
				depends = json.loads(depends)
			else:
				depends = {"campaigns": [], "file_patterns": []}
			try:
				if action == 'add':
					c = Campaign(name=name, experiment=exp,vo_role=vo_role,
								active=active, cs_split_type = split_type,
								software_version=software_version, dataset=dataset,
								param_overrides=param_overrides, launch_id=launch_id,
								campaign_definition_id=campaign_definition_id,
								creator=experimenter_id, created=datetime.now(utc))
					dbhandle.add(c)
					dbhandle.flush() ##### Is this flush() necessary or better a commit ?
					campaign_id = c.campaign_id
				else:
					columns = {
								"name":                  name,
								"vo_role":               vo_role,
								"active":                active,
								"cs_split_type":         split_type,
								"software_version":      software_version,
								"dataset" :              dataset,
								"param_overrides":       param_overrides,
								"campaign_definition_id":campaign_definition_id,
								"launch_id":             launch_id,
								"updated":               datetime.now(utc),
								"updater":               experimenter_id
								}
					cd = dbhandle.query(Campaign).filter(Campaign.campaign_id==campaign_id).update(columns)
			# now redo dependencies
				dbhandle.query(CampaignDependency).filter(CampaignDependency.uses_camp_id == campaign_id).delete()
				loghandle("depends for %s are: %s" % (campaign_id, depends))
				depcamps = dbhandle.query(Campaign).filter(Campaign.name.in_(depends['campaigns'])).all()
				for i in range(len(depcamps)):
					loghandle("trying to add dependency for: %s" % depcamps[i].name)
					d = CampaignDependency(uses_camp_id = campaign_id, needs_camp_id = depcamps[i].campaign_id, file_patterns=depends['file_patterns'][i])
					dbhandle.add(d)
				dbhandle.commit()
			except IntegrityError, e:
				message = "Integrity error - you are most likely using a name which already exists in database."
				loghandle(e.message)
				dbhandle.rollback()
			except SQLAlchemyError, e:
				message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
				loghandle(e.message)
				dbhandle.rollback()
			else:
				dbhandle.commit()

		# Find campaigns
		if exp: # cuz the default is find
			# for testing ui...
			#data['authorized'] = True
			state = kwargs.pop('state',None)
			if state == None:
				state = sesshandle.get('campaign_edit.state','state_active')
			sesshandle['campaign_edit.state'] = state
			data['state'] = state
			data['curr_experiment'] = exp
			data['authorized'] = sesshandle.get('experimenter').is_authorized(exp)
			cquery = dbhandle.query(Campaign).filter(Campaign.experiment==exp)
			if state == 'state_active':
				cquery = cquery.filter(Campaign.active==True)
			elif state == 'state_inactive':
				cquery = cquery.filter(Campaign.active==False)
			cquery = cquery.order_by(Campaign.name)
			data['campaigns'] = cquery
			data['definitions'] = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.experiment==exp).order_by(CampaignDefinition.name)
			data['templates'] = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).order_by(LaunchTemplate.name)
			cids = [c.campaign_id for c in data['campaigns'].all()]
			depends = {}
			for cid in cids:
				sql = (db.query(CampaignDependency.uses_camp_id, Campaign.name, CampaignDependency.file_patterns )
						.filter(CampaignDependency.uses_camp_id == cid,
						Campaign.campaign_id == CampaignDependency.needs_camp_id))
				deps = {
						"campaigns"     : [row[1] for row  in sql.all()],
						"file_patterns" : [row[2] for row  in sql.all()]
						}
				depends[cid] = json.dumps(deps)
			data['depends'] = depends
		data['message'] = message
		return data

	def campaign_edit_query(self, dbhandle, *args, **kwargs):
		data = {}
		ae_launch_id = kwargs.pop('ae_launch_id',None)
		ae_campaign_definition_id = kwargs.pop('ae_campaign_definition_id',None)

		if ae_launch_id:
			template = {}
			temp = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.launch_id==ae_launch_id).first()
			template['launch_host'] = temp.launch_host
			template['launch_account'] = temp.launch_account
			template['launch_setup'] = temp.launch_setup
			data['template'] = template

		if ae_campaign_definition_id:
			definition = {}
			cdef = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==ae_campaign_definition_id).first()
			definition['input_files_per_job'] = cdef.input_files_per_job
			definition['output_files_per_job'] = cdef.output_files_per_job
			definition['launch_script'] = cdef.launch_script
			definition['definition_parameters'] = cdef.definition_parameters
			data['definition'] = definition
		return json.dumps(data)
