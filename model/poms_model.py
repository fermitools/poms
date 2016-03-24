# coding: utf-8
from sqlalchemy import Table, BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String, Text, text, Float
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql.json import JSON
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata


class Campaign(Base):
    __tablename__ = 'campaigns'

    campaign_id = Column(Integer, primary_key=True, server_default=text("nextval('campaigns_campaign_id_seq'::regclass)"))
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    name = Column(Text, nullable=False)
    campaign_definition_id = Column(ForeignKey(u'campaign_definitions.campaign_definition_id'), nullable=False, index=True, server_default=text("nextval('campaigns_campaign_definition_id_seq'::regclass)"))
    creator = Column(ForeignKey(u'experimenters.experimenter_id'), nullable=False, index=True)
    created = Column(DateTime(True), nullable=False)
    updater = Column(ForeignKey(u'experimenters.experimenter_id'), index=True)
    updated = Column(DateTime(True))
    vo_role = Column(Text, nullable = False)
    cs_last_split = Column(DateTime(True), nullable=True)
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

    def __init__(self,first_name=None, last_name=None, email=None):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email

    experimenter_id = Column(Integer, primary_key=True, server_default=text("nextval('experimenters_experimenter_id_seq'::regclass)"))
    first_name = Column(Text, nullable=False)
    last_name = Column(Text)
    email = Column(Text, nullable=False)

            
class ExperimentsExperimenters(Base):
    __tablename__ = 'experiments_experimenters'

    def __init__(self, experimenter_id=None, experiment=None, active=None):
        self.experimenter_id = experimenter_id
        self.experiment = experiment
        self.active = active

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
    output_file_names = Column(Text)
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
    
    def __init__(self,launch_id=None, name=None, experiment=None, launch_host=None, launch_account=None, launch_setup=None,
                 creator=None, created=None, updater=None, updated=None):
        self.launch_id = launch_id
        self.name = name
        self.experiment = experiment
        self.launch_host = launch_host
        self.launch_account = launch_account
        self.launch_setup = launch_setup
        self.creator = creator
        self.created = created
        self.updater = updater
        self.updated = updated
        
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

    def __init__(self, campaign_definition_id=None, name=None, experiment=None, launch_script=None, definition_parameters=None, creator=None, created=None,
                 updater=None, updated=None, input_files_per_job = None, output_files_per_job = None ):
        self.campaign_definition_id = campaign_definition_id
        self.name = name
        self.experiment = experiment
        self.launch_script = launch_script
        self.definition_parameters = definition_parameters
        self.creator = creator
        self.created = created
        self.updater = updater
        self.updated = updated
        self.input_files_per_job = input_files_per_job
        self.output_files_per_job = output_files_per_job
        
    campaign_definition_id = Column(Integer, primary_key=True, server_default=text("nextval('campaign_definitions_campaign_definition_id_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    launch_script = Column(Text)
    definition_parameters = Column(JSON)
    input_files_per_job = Column(Integer)
    output_files_per_job = Column(Integer)
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
    #campaign_definition_id = Column(Integer, nullable=False, index=True, server_default=text("nextval('tasks_campaign_definition_id_seq'::regclass)"))
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

    campaign_obj = relationship(u'Campaign')
    experimenter_creator_obj = relationship(u'Experimenter', primaryjoin='Task.creator == Experimenter.experimenter_id')
    experimenter_updater_obj = relationship(u'Experimenter', primaryjoin='Task.updater == Experimenter.experimenter_id')
    parent_obj = relationship(u'Task', remote_side=[task_id])
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
    
    job_obj = relationship(u'Job',backref='history')

class Tag(Base):
    __tablename__ = 'tags'

    def __init__(self, experiment=None, tag_name=None):
        self.experiment = experiment
        self.tag_name = tag_name
    
    tag_id = Column(Integer, primary_key=True, server_default=text("nextval('tags_tag_id_seq'::regclass)"))
    experiment = Column(ForeignKey(u'experiments.experiment'), nullable=False, index=True)
    tag_name = Column(Text, nullable=False)
    
class CampaignsTags(Base):
    __tablename__ = 'campaigns_tags'

    def __init__(self, campaign_id=None, tag_id=None):
        self.campaign_id = campaign_id
        self.tag_id = tag_id

    campaign_id = Column(Integer, ForeignKey('campaigns.campaign_id'), primary_key=True)
    tag_id = Column(Integer, ForeignKey('tags.tag_id'), primary_key=True)

    campaign_obj = relationship(Campaign, backref="campaigns_tags")
    tag_obj = relationship(Tag, backref="campaigns_tags")
