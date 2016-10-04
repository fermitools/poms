# coding: utf-8
from sqlalchemy import Table, BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String, Text, text, Float
from sqlalchemy.orm import relationship, backref
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Campaign(Base):
    __tablename__ = 'campaigns'

    campaign_id = Column(Integer, primary_key=True, server_default=text("nextval('campaigns_campaign_id_seq'::regclass)"))
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    name = Column(Text, nullable=False)
    campaign_definition_id = Column(ForeignKey(u'campaign_definitions.campaign_definition_id'), nullable=False, index=True,
                                    server_default=text("nextval('campaigns_campaign_definition_id_seq'::regclass)"))
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey(u'experimenters.experimenter_id'), index=True)
    updated = Column(DateTime(True))
    vo_role = Column(Text, nullable = False)
    cs_last_split = Column(Integer, nullable=True)
    cs_split_type = Column(Text, nullable = True)
    cs_split_dimensions = Column(Text, nullable = True)
    dataset = Column(Text, nullable = False)
    software_version = Column(Text, nullable = False)
    active = Column(Boolean, nullable=False, server_default=text("true"))
    launch_id = Column(ForeignKey(u'launch_templates.launch_id'), nullable=False)
    param_overrides = Column(JSON)

    experimenter_creator_obj = relationship(u'Experimenter', primaryjoin='Campaign.creator == Experimenter.experimenter_id')
    experimenter_updater_obj = relationship(u'Experimenter', primaryjoin='Campaign.updater == Experimenter.experimenter_id')
    experiment_obj = relationship(u'Experiment')
    campaign_definition_obj = relationship(u'CampaignDefinition')
    launch_template_obj = relationship(u'LaunchTemplate')


class Experimenter(Base):
    __tablename__ = 'experimenters'

    experimenter_id = Column(Integer, primary_key=True, server_default=text("nextval('experimenters_experimenter_id_seq'::regclass)"))
    first_name = Column(Text, nullable=False)
    last_name = Column(Text)
    email = Column(Text, nullable=False)

            
class ExperimentsExperimenters(Base):
    __tablename__ = 'experiments_experimenters'

    experimenter_id = Column(Integer, ForeignKey('experimenters.experimenter_id'), primary_key=True)
    experiment = Column(Text, ForeignKey('experiments.experiment'), primary_key=True)
    active = Column(Boolean, nullable=False, server_default=text("true"))

    experimenter_obj = relationship(Experimenter, backref="exp_expers")
    experiment_obj   = relationship("Experiment", backref="exp_expers")


class Experiment(Base):
    __tablename__ = 'experiments'

    experiment = Column(String(10), primary_key=True)
    name = Column(Text, nullable=False)
    

class Job(Base): 
    __tablename__ = 'jobs'

    job_id = Column(BigInteger, primary_key=True, server_default=text("nextval('jobs_job_id_seq'::regclass)"))
    task_id = Column(ForeignKey(u'tasks.task_id'), nullable=False, index=True)
    jobsub_job_id = Column(Text, nullable=False)
    node_name = Column(Text, nullable=False)
    cpu_type = Column(Text, nullable=False)
    host_site = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    updated = Column(DateTime(True), nullable=False)
    output_files_declared = Column(Boolean, nullable=False)
    user_exe_exit_code = Column(Integer)
    input_file_names = Column(Text)
    reason_held = Column(Text)
    consumer_id = Column(Text)
    cpu_time = Column(Float)
    wall_time = Column(Float)

    task_obj = relationship(u'Task')


class ServiceDowntime(Base):
    __tablename__ = 'service_downtimes'

    service_id = Column(ForeignKey(u'services.service_id'), primary_key=True, nullable=False)
    downtime_started = Column(DateTime(True), primary_key=True, nullable=False)
    downtime_ended = Column(DateTime(True), nullable=True)
    downtime_type = Column(Text, nullable=False)

    service_obj = relationship(u'Service')


class Service(Base):
    __tablename__ = 'services'

    service_id = Column(Integer, primary_key=True, server_default=text("nextval('services_service_id_seq'::regclass)"))
    name = Column(Text, nullable=False)
    host_site = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    updated = Column(DateTime(True), nullable=False)
    active = Column(Boolean, nullable=False, server_default=text("true"))
    parent_service_id = Column(ForeignKey(u'services.service_id'), index=True)
    url = Column(Text)
    items = Column(Integer)
    failed_items = Column(Integer)
    description = Column(Text)

    parent_service_obj = relationship(u'Service', remote_side=[service_id])


class LaunchTemplate(Base):
    __tablename__ = 'launch_templates'
    
    launch_id = Column(Integer, primary_key=True, server_default=text("nextval('launch_templates_launch_id_seq'::regclass)"))
    name = Column(Text, nullable=False, index=True, unique=True)
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    launch_host = Column(Text, nullable=False)
    launch_account = Column(Text, nullable=False)
    launch_setup = Column(Text, nullable=False)
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey(u'experimenters.experimenter_id'), index=True)
    updated = Column(DateTime(True))

    experiment_obj = relationship(u'Experiment')
    experimenter_creator_obj = relationship(u'Experimenter', primaryjoin='LaunchTemplate.creator == Experimenter.experimenter_id')
    experimenter_updater_obj = relationship(u'Experimenter', primaryjoin='LaunchTemplate.updater == Experimenter.experimenter_id')
    

class CampaignDefinition(Base):
    __tablename__ = 'campaign_definitions'

    campaign_definition_id = Column(Integer, primary_key=True, server_default=text("nextval('campaign_definitions_campaign_definition_id_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    launch_script = Column(Text)
    definition_parameters = Column(JSON)
    input_files_per_job = Column(Integer)
    output_files_per_job = Column(Integer)
    output_file_patterns = Column(Text)
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey(u'experimenters.experimenter_id'), index=True)
    updated = Column(DateTime(True))


    experiment_obj = relationship(u'Experiment')
    experimenter_creator_obj = relationship(u'Experimenter', primaryjoin='CampaignDefinition.creator == Experimenter.experimenter_id')
    experimenter_updater_obj = relationship(u'Experimenter', primaryjoin='CampaignDefinition.updater == Experimenter.experimenter_id')


class Task(Base):
    __tablename__ = 'tasks'

    task_id = Column(Integer, primary_key=True, server_default=text("nextval('tasks_task_id_seq'::regclass)"))
    campaign_id = Column(ForeignKey(u'campaigns.campaign_id'), nullable=False, index=True, server_default=text("nextval('tasks_campaign_id_seq'::regclass)"))
    task_order = Column(Integer, nullable=False)
    input_dataset = Column(Text, nullable=False)
    output_dataset = Column(Text, nullable=False)
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    status = Column(Text, nullable=False)
    task_parameters = Column(JSON)
    depends_on = Column(ForeignKey(u'tasks.task_id'), index=True)
    depend_threshold = Column(Integer)
    updater = Column(ForeignKey(u'experimenters.experimenter_id'), index=True)
    updated = Column(DateTime(True))
    command_executed = Column(Text)
    project = Column(Text)
    launch_snapshot_id = Column(ForeignKey(u'launch_template_snapshots.launch_snapshot_id'), nullable=True, index=True)
    campaign_snapshot_id = Column(ForeignKey(u'campaign_snapshots.campaign_snapshot_id'), nullable=True, index=True)
    campaign_definition_snap_id = Column(ForeignKey(u'campaign_definition_snapshots.campaign_definition_snap_id'), nullable=True, index=True)
    recovery_position = Column(Integer)
    recovery_tasks_parent = Column(ForeignKey(u'tasks.task_id'),index=True)

    campaign_obj = relationship(u'Campaign')
    experimenter_creator_obj = relationship(u'Experimenter', primaryjoin='Task.creator == Experimenter.experimenter_id')
    experimenter_updater_obj = relationship(u'Experimenter', primaryjoin='Task.updater == Experimenter.experimenter_id')
    parent_obj = relationship(u'Task', remote_side=[task_id],foreign_keys=recovery_tasks_parent)
    launch_template_snap_obj = relationship(u'LaunchTemplateSnapshot', foreign_keys=launch_snapshot_id)
    campaign_snap_obj = relationship(u'CampaignSnapshot', foreign_keys=campaign_snapshot_id)
    campaign_definition_snap_obj = relationship(u'CampaignDefinitionSnapshot', foreign_keys=campaign_definition_snap_id)
    jobs = relationship(u'Job', order_by = "Job.job_id")

class TaskHistory(Base):
    __tablename__ = 'task_histories'

    task_id = Column(ForeignKey(u'tasks.task_id'), primary_key=True, nullable=False)
    created = Column(DateTime(True), primary_key=True, nullable=False)
    status = Column(Text, nullable=False)
    
    task_obj = relationship(u'Task',backref='history')

class JobHistory(Base):
    __tablename__ = 'job_histories'

    
    job_id = Column(ForeignKey(u'jobs.job_id'), primary_key=True, nullable=False)
    created = Column(DateTime(True), primary_key=True, nullable=False)
    status = Column(Text, nullable=False)
    
    job_obj = relationship(u'Job',backref=backref('history',cascade="all,delete-orphan"))

class Tag(Base):
    __tablename__ = 'tags'

    tag_id = Column(Integer, primary_key=True, server_default=text("nextval('tags_tag_id_seq'::regclass)"))
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    tag_name = Column(Text, nullable=False)
    
class CampaignsTags(Base):
    __tablename__ = 'campaigns_tags'

    campaign_id = Column(Integer, ForeignKey('campaigns.campaign_id'), primary_key=True)
    tag_id = Column(Integer, ForeignKey('tags.tag_id'), primary_key=True)

    campaign_obj = relationship(Campaign, backref="campaigns_tags")
    tag_obj = relationship(Tag, backref="campaigns_tags")

class JobFile(Base):
    __tablename__ = 'job_files'

    job_id = Column(Integer, ForeignKey('jobs.job_id'), primary_key=True)
    file_name = Column(Text, primary_key=True, nullable=False)
    file_type = Column(Text, nullable=False)
    created = Column(DateTime(True), nullable=False)
    declared = Column(DateTime(True))
    
    job_obj = relationship(Job, backref=backref('job_files', cascade="all,delete-orphan"))
    

class CampaignSnapshot(Base):
    __tablename__ = 'campaign_snapshots'

    campaign_snapshot_id = Column(Integer, primary_key=True, server_default=text("nextval('campaign_snapshots_campaign_snapshot_id_seq'::regclass)"))
    campaign_id = Column(ForeignKey(u'campaigns.campaign_id'), nullable=False, index=True)
    experiment = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    campaign_definition_id = Column(Integer, nullable=False)
    vo_role = Column(Text, nullable=False)
    creator = Column(Integer, nullable=False)
    created = Column(DateTime(True), nullable=False)
    active = Column(Boolean, nullable=False, server_default=text("true"))
    dataset = Column(Text, nullable=False)
    software_version = Column(Text, nullable=False)
    launch_id = Column(Integer, nullable=False)
    param_overrides = Column(JSON)
    updater = Column(Integer)
    updated = Column(DateTime(True))
    cs_last_split = Column(Integer)
    cs_split_type = Column(Text)
    cs_split_dimensions = Column(Text)

    campaign = relationship(u'Campaign')


class CampaignDefinitionSnapshot(Base):
    __tablename__ = 'campaign_definition_snapshots'

    campaign_definition_snap_id = Column(Integer, primary_key=True, server_default=text("nextval('campaign_definition_snapshots_campaign_definition_snap_id_seq'::regclass)"))
    campaign_definition_id = Column(ForeignKey(u'campaign_definitions.campaign_definition_id'), nullable=False, index=True)
    name = Column(Text, nullable=False)
    experiment = Column(Text, nullable=False)
    launch_script = Column(Text)
    definition_parameters = Column(JSON)
    input_files_per_job = Column(Integer)
    output_files_per_job = Column(Integer)
    creator = Column(Integer, nullable=False)
    created = Column(DateTime(True), nullable=False)
    updater = Column(Integer)
    updated = Column(DateTime(True))

    campaign_definition = relationship(u'CampaignDefinition')


class LaunchTemplateSnapshot(Base):
    __tablename__ = 'launch_template_snapshots'

    launch_snapshot_id = Column(Integer, primary_key=True, server_default=text("nextval('launch_template_snapshots_launch_snapshot_id_seq'::regclass)"))
    launch_id = Column(ForeignKey(u'launch_templates.launch_id'), nullable=False, index=True)
    experiment = Column(String(10), nullable=False)
    launch_host = Column(Text, nullable=False)
    launch_account = Column(Text, nullable=False)
    launch_setup = Column(Text, nullable=False)
    creator = Column(Integer, nullable=False)
    created = Column(DateTime(True), nullable=False)
    updater = Column(Integer)
    updated = Column(DateTime(True))
    name = Column(Text, nullable=False)

    launch = relationship(u'LaunchTemplate')


class RecoveryType(Base):
    __tablename__ = 'recovery_types'

    recovery_type_id = Column(Integer, primary_key=True, server_default=text("nextval('recovery_types_recovery_type_id_seq'::regclass)"))
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=False)


class CampaignRecovery(Base):
    __tablename__ = 'campaign_recoveries'

    campaign_definition_id = Column(ForeignKey(u'campaign_definitions.campaign_definition_id'), primary_key=True, nullable=False)
    recovery_type_id = Column(ForeignKey(u'recovery_types.recovery_type_id'), primary_key=True, nullable=False, index=True)
    recovery_order = Column(Integer, nullable=False)

    campaign_definition = relationship(u'CampaignDefinition')
    recovery_type = relationship(u'RecoveryType')

class CampaignDependency(Base):
    __tablename__ = 'campaign_dependencies'

    campaign_dep_id =       Column(Integer, primary_key=True, server_default=text("nextval('campaign_dependency_id_seq'::regclass)"))
    needs_camp_id = Column(ForeignKey(u'campaigns.campaign_id'), primary_key=True, nullable=False, index=True)        
    uses_camp_id   = Column(ForeignKey(u'campaigns.campaign_id'), primary_key=True, nullable=False, index=True)              
    file_patterns  = Column(Text, nullable=False)

    needs_camp = relationship(u'Campaign',foreign_keys=needs_camp_id)
    uses_camp = relationship(u'Campaign',foreign_keys=uses_camp_id)
